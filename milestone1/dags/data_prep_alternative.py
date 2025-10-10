"""
Alternative Data Preparation Script for Apache Airflow
Creates patient-centric records with disease, symptom lists, text, and attributes
Output format: patient_id, disease, symptoms_list, text, chronic, contagious
"""

import pandas as pd
import os
import shutil
from pathlib import Path
import numpy as np


def setup_paths():
    base_path = Path(__file__).parent.parent / 'data'
    
    paths = {
        'original': base_path / 'original',
        'clean': base_path / 'clean',
        'final': base_path / 'final',
        'alternative': base_path / 'alternative'
    }
    
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    
    return paths


def clean_alternative_folder_task(**kwargs):
    """
    Task 0: Clean the alternative folder to start fresh
    """
    print("=" * 60)
    print("TASK 0: Cleaning Alternative Folder")
    print("=" * 60)
    
    paths = setup_paths()
    for _, path in paths.items():
        if path.exists():
            print(f"Removing existing alternative folder: {path}")
            shutil.rmtree(path)
            print(f"✓ Removed existing alternative folder")

    
    paths['alternative'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh alternative folder: {paths['alternative']}")
    
    return {
        'alternative_folder': str(paths['alternative']),
        'status': 'cleaned'
    }


def clean_datasets_task(**kwargs):
    """
    Task 1: Clean all datasets from original folder - remove NA and duplicates
    Save cleaned datasets to clean folder
    """
    print("=" * 60)
    print("TASK 1: Cleaning Original Datasets")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Clean the clean folder first
    if paths['clean'].exists():
        print(f"Removing existing clean folder: {paths['clean']}")
        shutil.rmtree(paths['clean'])
    
    paths['clean'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh clean folder: {paths['clean']}")
    
    # Get all CSV files from original folder
    csv_files = list(paths['original'].glob('*.csv'))
    
    cleaning_stats = {}
    
    for csv_file in csv_files:
        print(f"\nCleaning {csv_file.name}...")
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            original_rows = len(df)
            original_cols = len(df.columns)

            # Normalize column names
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

            # Remove rows with all NA values
            df = df.dropna(how='all')
            after_all_na = len(df)
            
            # Remove duplicate rows
            df = df.drop_duplicates()
            after_duplicates = len(df)
            
            # Save cleaned dataset to clean folder
            output_file = paths['clean'] / csv_file.name
            df.to_csv(output_file, index=False)
            
            # Store statistics
            cleaning_stats[csv_file.name] = {
                'original_rows': original_rows,
                'original_cols': original_cols,
                'after_removing_all_na': after_all_na,
                'after_removing_duplicates': after_duplicates,
                'rows_removed_na': original_rows - after_all_na,
                'rows_removed_duplicates': after_all_na - after_duplicates,
                'final_rows': after_duplicates,
                'total_rows_removed': original_rows - after_duplicates,
                'percentage_removed': ((original_rows - after_duplicates) / original_rows * 100) if original_rows > 0 else 0
            }
            
            print(f"  ✓ Original rows: {original_rows:,}")
            print(f"  ✓ Rows with all NA removed: {original_rows - after_all_na:,}")
            print(f"  ✓ Duplicate rows removed: {after_all_na - after_duplicates:,}")
            print(f"  ✓ Final rows: {after_duplicates:,} ({cleaning_stats[csv_file.name]['percentage_removed']:.2f}% removed)")
            print(f"  ✓ Saved to: {output_file.name}")
            
        except Exception as e:
            print(f"  ✗ Error cleaning {csv_file.name}: {str(e)}")
            cleaning_stats[csv_file.name] = {'error': str(e)}
    
    print(f"\n✓ Cleaned {len(csv_files)} datasets")
    
    return {
        'total_files_cleaned': len(csv_files),
        'cleaning_stats': cleaning_stats
    }


def process_disease_attributes_task(**kwargs):
    """
    Task 2: Process disease attributes (chronic, contagious) from Diseases_Symptoms.csv
    """
    print("=" * 60)
    print("TASK 2: Processing Disease Attributes")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Load disease attributes
    diseases_df = pd.read_csv(paths['clean'] / "Diseases_Symptoms.csv")
    
    # Check for column names (normalized)
    disease_col = [col for col in diseases_df.columns if col.lower() in ['name', 'disease', 'disease_name']][0] if any(col.lower() in ['name', 'disease', 'disease_name'] for col in diseases_df.columns) else diseases_df.columns[0]
    contagious_col = [col for col in diseases_df.columns if 'contagious' in col.lower()][0] if any('contagious' in col.lower() for col in diseases_df.columns) else 'contagious'
    chronic_col = [col for col in diseases_df.columns if 'chronic' in col.lower()][0] if any('chronic' in col.lower() for col in diseases_df.columns) else 'chronic'
    
    # Select and rename relevant columns
    disease_attributes = diseases_df[[disease_col, contagious_col, chronic_col]].copy()
    disease_attributes.columns = ['disease', 'contagious', 'chronic']
    
    # Normalize disease names
    disease_attributes['disease'] = disease_attributes['disease'].str.lower().str.strip()
    
    # Remove duplicates based on disease
    disease_attributes = disease_attributes.drop_duplicates(subset=['disease'])
    
    # Convert boolean to int (0/1)
    disease_attributes['contagious'] = disease_attributes['contagious'].astype(int)
    disease_attributes['chronic'] = disease_attributes['chronic'].astype(int)
    
    # Save to alternative folder
    disease_attributes.to_csv(paths['alternative'] / "disease_attributes.csv", index=False)
    
    print(f"✓ Processed {len(disease_attributes)} unique diseases")
    print(f"✓ Contagious diseases: {disease_attributes['contagious'].sum()}")
    print(f"✓ Chronic diseases: {disease_attributes['chronic'].sum()}")
    
    return {
        'total_diseases': len(disease_attributes),
        'contagious_count': int(disease_attributes['contagious'].sum()),
        'chronic_count': int(disease_attributes['chronic'].sum())
    }

def add_missing_columns_task(**kwargs):
    """
    Task 3: Ensure all required columns are present in the final dataset
    Required columns: patient_id, disease, symptoms_list, text, chronic, contagious
    """
    print("=" * 60)
    print("TASK 3: Adding Missing Columns to Final Dataset")
    print("=" * 60)
    
    paths = setup_paths()

    files = list(paths['clean'].glob('*.csv'))
    
    final_file = paths['clean'] / "Patient_Disease_Symptoms.csv"
    
    if not final_file.exists():
        print(f"✗ Final dataset not found: {final_file}")
        return {'status': 'final_dataset_not_found'}
    
    df = pd.read_csv(final_file)
    
    # Normalize column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    
    # Add missing columns with default values
    df = add_columns_to_df(df)
    
    # Save updated dataset to alternative folder
    output_file = paths['alternative'] / "patient_disease_symptoms_final.csv"
    df.to_csv(output_file, index=False)
    
    print(f"✓ Added missing columns where necessary")
    print(f"✓ Saved updated dataset to: {output_file}")
    
    return {
        'total_rows': len(df),
        'output_file': str(output_file)
    }

def add_columns_to_df(df):
    columns = {
        'patient_id': '',
        'disease': '',
        'symptoms_list': '',
        'text': '',
        'chronic': None,
        'contagious': None
    }
    for col_name, default_value in columns.items():
        if col_name not in df.columns:
            df[col_name] = default_value
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("Running Alternative Data Preparation Script")
    print("This is for standalone testing outside of Airflow")
    print("Each task will run sequentially")
    print("=" * 60)
    
    # Run each task sequentially
    clean_alternative_folder_task()
    clean_datasets_task()
    process_disease_attributes_task()
    add_missing_columns_task()
    
    print("=" * 60)
    print("All tasks completed.")
    print(f"Check the 'alternative' folder for outputs.")
    print("=" * 60)
