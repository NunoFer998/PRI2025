"""
Data Preparation and Cleaning Script for Apache Airflow
Transforms and merges multiple disease and symptom datasets
"""

import pandas as pd
import os
import shutil
from pathlib import Path


def setup_paths():
    """
    Setup and validate data paths
    Returns: dict with data paths
    """
    base_path = Path(__file__).parent.parent / 'data'
    
    paths = {
        'original': base_path / 'original',
        'clean': base_path / 'clean',
        'final': base_path / 'final'
    }
    
    # Create directories if they don't exist
    paths['clean'].mkdir(parents=True, exist_ok=True)
    paths['final'].mkdir(parents=True, exist_ok=True)
    
    return paths


def clean_folders_task(**kwargs):
    """
    Task 0: Clean the clean folder to start fresh
    """
    print("=" * 60)
    print("TASK 0: Cleaning Folders")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Remove and recreate clean folder
    if paths['clean'].exists():
        print(f"Removing existing clean folder: {paths['clean']}")
        shutil.rmtree(paths['clean'])
        print(f"✓ Removed existing clean folder")
    
    paths['clean'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh clean folder: {paths['clean']}")
    
    return {
        'clean_folder': str(paths['clean']),
        'status': 'cleaned'
    }


def separate_symptoms_task(**kwargs):
    """
    Task 1: Separate symptoms from disease_symptom_list.csv and diseases_with_symptoms.csv
    """
    print("=" * 60)
    print("TASK 1: Separating Symptoms")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Separate symptoms from disease_symptom_list.csv
    print("Processing disease_symptom_list.csv...")
    symptoms_separated1 = pd.read_csv(paths['original'] / "disease_symptom_list.csv")
    symptoms_separated1 = symptoms_separated1.assign(
        symptom=symptoms_separated1['symptoms'].str.split(', ')
    ).explode('symptom')
    symptoms_separated1['symptom'] = symptoms_separated1['symptom'].str.strip()
    symptoms_separated1 = symptoms_separated1.drop(columns=['symptoms'])
    symptoms_separated1.to_csv(paths['clean'] / "symptoms_separated.csv", index=False)
    print(f"✓ Created symptoms_separated.csv with {len(symptoms_separated1)} rows")
    
    # Separate symptoms from diseases_with_symptoms.csv
    print("Processing diseases_with_symptoms.csv...")
    symptoms_separated2 = pd.read_csv(paths['original'] / "diseases_with_symptoms.csv")
    symptoms_separated2 = symptoms_separated2.assign(
        symptom=symptoms_separated2['symptoms'].str.split(', ')
    ).explode('symptom')
    symptoms_separated2['symptom'] = symptoms_separated2['symptom'].str.strip()
    symptoms_separated2 = symptoms_separated2[['disease', 'symptom']]
    symptoms_separated2.to_csv(paths['clean'] / "symptoms_separated2.csv", index=False)
    print(f"✓ Created symptoms_separated2.csv with {len(symptoms_separated2)} rows")
    
    return {
        'symptoms_separated1_rows': len(symptoms_separated1),
        'symptoms_separated2_rows': len(symptoms_separated2)
    }


def merge_separated_symptoms_task(**kwargs):
    """
    Task 2: Merge both separated symptom files and remove duplicates
    """
    print("=" * 60)
    print("TASK 2: Merging Separated Symptoms")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load the separated symptoms
    separated_symptoms1 = pd.read_csv(paths['clean'] / "symptoms_separated.csv")
    separated_symptoms2 = pd.read_csv(paths['clean'] / "symptoms_separated2.csv")
    
    # Merge both CSVs
    all_separated_symptoms = pd.concat([separated_symptoms1, separated_symptoms2], ignore_index=True)
    all_separated_symptoms.to_csv(paths['clean'] / "all_separated_symptoms.csv", index=False)
    
    print(f"✓ Merged symptoms files")
    print(f"  Total rows before deduplication: {len(all_separated_symptoms)}")
    print(f"  Unique diseases: {all_separated_symptoms['disease'].nunique()}")
    
    return {

        'unique_diseases': all_separated_symptoms['disease'].nunique(),
    }


def merge_with_patient_reports_task(**kwargs):
    """
    Task 3: Merge separated symptoms with patient reports
    """
    print("=" * 60)
    print("TASK 3: Merging with Patient Reports")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load data
    all_separated_symptoms = pd.read_csv(paths['clean'] / "all_separated_symptoms.csv")
    patients_reports = pd.read_csv(paths['original'] / "patient_reports.csv")
    
    # Process patient reports
    patients_reports = patients_reports.rename(columns={'label': 'disease'})
    patients_reports['symptom'] = ""
    patients_reports = patients_reports[['disease', 'symptom', 'text']]
    
    # Add text column to symptoms
    all_separated_symptoms['text'] = ""
    
    # Merge
    merged_symptoms_text = pd.concat([all_separated_symptoms, patients_reports], ignore_index=True)
    merged_symptoms_text['disease'] = merged_symptoms_text['disease'].str.lower().str.strip()
    merged_symptoms_text = merged_symptoms_text.sort_values(by='disease').reset_index(drop=True)
    
    merged_symptoms_text.to_csv(paths['clean'] / "merged_symptoms_text.csv", index=False)
    
    print(f"✓ Merged with patient reports")
    print(f"  Total rows: {len(merged_symptoms_text)}")
    print(f"  Patient report rows: {len(patients_reports)}")
    print(f"  Symptom rows: {len(all_separated_symptoms)}")
    
    return {
        'total_rows': len(merged_symptoms_text),
        'patient_report_rows': len(patients_reports),
        'symptom_rows': len(all_separated_symptoms)
    }


def process_train_dataset_task(**kwargs):
    """
    Task 4: Process train dataset and merge with existing data
    """
    print("=" * 60)
    print("TASK 4: Processing Train Dataset")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load data
    train01 = pd.read_csv(paths['original'] / "train-00000-of-00001.csv")
    merged_symptoms_text = pd.read_csv(paths['clean'] / "merged_symptoms_text.csv")
    
    # Rename columns
    train01 = train01.rename(columns={
        'Source_URL': 'source_url',
        'Disease_Name': 'disease',
        'Symptom_List': 'symptom',
        'Generated_Sentence_From_symptoms': 'text'
    })
    
    # Remove unnamed columns
    train01 = train01.loc[:, ~train01.columns.str.contains('^Unnamed')]
    train01['disease'] = train01['disease'].str.lower().str.strip()
    
    # Explode symptoms
    train01 = train01.assign(symptom=train01['symptom'].str.split(' \| ')).explode('symptom')
    train01['symptom'] = train01['symptom'].str.strip()
    
    # Add source_url column if missing
    if 'source_url' not in merged_symptoms_text.columns:
        merged_symptoms_text['source_url'] = None
    
    # Align columns
    train01 = train01[['disease', 'symptom', 'text', 'source_url']]
    merged_symptoms_text = merged_symptoms_text[['disease', 'symptom', 'text', 'source_url']]
    
    # Merge
    final = pd.concat([merged_symptoms_text, train01], ignore_index=True)
    final['disease'] = final['disease'].str.lower().str.strip()
    final = final.sort_values(by='disease').reset_index(drop=True)
    
    final.to_csv(paths['clean'] / "final.csv", index=False)
    
    print(f"✓ Processed train dataset")
    print(f"  Train rows: {len(train01)}")
    print(f"  Merged symptoms text rows: {len(merged_symptoms_text)}")
    print(f"  Final rows: {len(final)}")
    print(f"  Unique diseases: {final['disease'].nunique()}")
    
    return {
        'train_rows': len(train01),
        'merged_rows': len(merged_symptoms_text),
        'final_rows': len(final),
        'unique_diseases': final['disease'].nunique()
    }


def process_treatment_data_task(**kwargs):
    """
    Task 5: Process diseases with treatment data
    """
    print("=" * 60)
    print("TASK 5: Processing Treatment Data")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load data
    diseases_symptoms_with_treatment = pd.read_csv(paths['original'] / "Diseases_Symptoms.csv")
    
    # Drop Disease_Code column
    diseases_with_treatment = diseases_symptoms_with_treatment.drop('Disease_Code', axis=1)
    
    # Convert to lowercase
    diseases_with_treatment = diseases_with_treatment.apply(
        lambda x: x.str.lower() if x.dtype == "object" else x
    )
    
    # Rename columns
    diseases_with_treatment = diseases_with_treatment.rename(columns={
        'Name': 'disease',
        'Symptoms': 'symptom',
        'Treatments': 'treatment',
        'Contagious': 'contagious',
        'Chronic': 'chronic'
    })
    
    # Convert boolean columns to binary (0/1)
    boolean_columns = ['contagious', 'chronic']
    diseases_with_treatment[boolean_columns] = diseases_with_treatment[boolean_columns].astype(int)
    
    diseases_with_treatment.to_csv(paths['clean'] / "diseases_with_treatment.csv", index=False)
    
    print(f"✓ Processed treatment data")
    print(f"  Total rows: {len(diseases_with_treatment)}")
    print(f"  Unique diseases: {diseases_with_treatment['disease'].nunique()}")
    print(f"  Contagious diseases: {diseases_with_treatment['contagious'].sum()}")
    print(f"  Chronic diseases: {diseases_with_treatment['chronic'].sum()}")
    
    return {
        'total_rows': len(diseases_with_treatment),
        'unique_diseases': diseases_with_treatment['disease'].nunique(),
        'contagious_count': int(diseases_with_treatment['contagious'].sum()),
        'chronic_count': int(diseases_with_treatment['chronic'].sum())
    }


def create_final_dataset_task(**kwargs):
    """
    Task 6: Create final merged dataset
    """
    print("=" * 60)
    print("TASK 6: Creating Final Dataset")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load data
    df_1 = pd.read_csv(paths['clean'] / "final.csv")
    df_2 = pd.read_csv(paths['clean'] / "diseases_with_treatment.csv")
    
    # Normalize disease and symptom columns
    for df in [df_1, df_2]:
        df['disease'] = df['disease'].astype(str).str.strip().str.lower()
        df['symptom'] = df['symptom'].astype(str).str.strip().str.lower()
        df['symptom'] = df['symptom'].str.strip().str.strip('"')
    
    # Merge datasets
    final_dataset = pd.merge(df_1, df_2, on=['disease', 'symptom'], how='outer')
    final_dataset = final_dataset.fillna('')
    final_dataset = final_dataset.sort_values(by='disease')
    
    final_dataset.to_csv(paths['final'] / "merged_final_dataset.csv", index=False)
    
    print(f"✓ Created final dataset")
    print(f"  Total rows: {len(final_dataset)}")
    print(f"  Total columns: {len(final_dataset.columns)}")
    print(f"  Unique diseases: {final_dataset['disease'].nunique()}")
    print(f"  Columns: {', '.join(final_dataset.columns)}")
    
    # Print sample
    print("\nSample data (first 3 rows):")
    print(final_dataset.head(3).to_string(index=False))
    
    return {
        'total_rows': len(final_dataset),
        'total_columns': len(final_dataset.columns),
        'unique_diseases': final_dataset['disease'].nunique(),
        'output_file': str(paths['final'] / "merged_final_dataset.csv")
    }


def run_all_tasks():
    """
    Run all tasks sequentially (for standalone execution)
    """
    print("\n" + "=" * 60)
    print("DATA PREPARATION AND CLEANING PIPELINE")
    print("=" * 60 + "\n")
    
    results = {}
    
    # Execute tasks in order (starting with cleaning)
    results['task0'] = clean_folders_task()
    results['task1'] = separate_symptoms_task()
    results['task2'] = merge_separated_symptoms_task()
    results['task3'] = merge_with_patient_reports_task()
    results['task4'] = process_train_dataset_task()
    results['task5'] = process_treatment_data_task()
    results['task6'] = create_final_dataset_task()
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    # Print summary
    print("\nPIPELINE SUMMARY:")
    print("-" * 60)
    for task_name, task_result in results.items():
        print(f"\n{task_name.upper()}:")
        for key, value in task_result.items():
            print(f"  {key}: {value}")
    
    return results


if __name__ == "__main__":
    run_all_tasks()
