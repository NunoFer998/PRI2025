from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_preparation_pipeline',
    default_args=default_args,
    description='Data preparation and cleaning pipeline for disease symptoms',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define base paths - adjust these based on your Docker volume mounts
BASE_DIR = '/opt/airflow/dags'
DATA_DIR = os.path.join(BASE_DIR, '../data')
CLEAN_DIR = os.path.join(DATA_DIR, 'clean')
FINAL_DIR = os.path.join(DATA_DIR, 'final')

def create_directories():
    """Create necessary directories if they don't exist"""
    os.makedirs(CLEAN_DIR, exist_ok=True)
    os.makedirs(FINAL_DIR, exist_ok=True)
    print(f"Created directories: {CLEAN_DIR}, {FINAL_DIR}")

def separate_symptoms_task1():
    """Separate each symptom from disease_symptom_list.csv"""
    print("Starting to separate symptoms from disease_symptom_list.csv...")
    symptoms_separated1 = pd.read_csv(f"{DATA_DIR}/disease_symptom_list.csv")
    symptoms_separated1 = symptoms_separated1.assign(symptom=symptoms_separated1['symptoms'].str.split(', ')).explode('symptom')
    symptoms_separated1['symptom'] = symptoms_separated1['symptom'].str.strip()
    symptoms_separated1 = symptoms_separated1.drop(columns=['symptoms'])
    symptoms_separated1.to_csv(f"{DATA_DIR}/symptoms_separated.csv", index=False)
    print(f"✓ Separated {len(symptoms_separated1)} symptom entries from disease_symptom_list.csv")

def separate_symptoms_task2():
    """Separate each symptom from diseases_with_symptoms.csv"""
    print("Starting to separate symptoms from diseases_with_symptoms.csv...")
    symptoms_separated2 = pd.read_csv(f"{DATA_DIR}/diseases_with_symptoms.csv")
    symptoms_separated2 = symptoms_separated2.assign(symptom=symptoms_separated2['symptoms'].str.split(', ')).explode('symptom')
    symptoms_separated2['symptom'] = symptoms_separated2['symptom'].str.strip()
    symptoms_separated2 = symptoms_separated2[['disease', 'symptom']]
    symptoms_separated2.to_csv(f"{DATA_DIR}/symptoms_separated2.csv", index=False)
    print(f"✓ Separated {len(symptoms_separated2)} symptom entries from diseases_with_symptoms.csv")

def merge_separated_symptoms():
    """Merge both separated symptom CSVs and remove duplicates"""
    print("Merging separated symptoms...")
    separated_symptoms1 = pd.read_csv(f"{DATA_DIR}/symptoms_separated.csv")
    separated_symptoms2 = pd.read_csv(f"{DATA_DIR}/symptoms_separated2.csv")
    
    all_separated_symptoms = pd.concat([separated_symptoms1, separated_symptoms2], ignore_index=True)
    all_separated_symptoms = all_separated_symptoms.drop_duplicates()
    all_separated_symptoms.to_csv(f"{CLEAN_DIR}/all_separated_symptoms.csv", index=False)
    
    diseases = all_separated_symptoms['disease'].unique()
    print(f"✓ Merged symptoms: {len(all_separated_symptoms)} unique disease-symptom pairs")
    print(f"✓ Found {len(diseases)} unique diseases")

def merge_with_patient_reports():
    """Merge symptoms with patient reports"""
    print("Merging with patient reports...")
    all_separated_symptoms = pd.read_csv(f"{CLEAN_DIR}/all_separated_symptoms.csv")
    patients_reports = pd.read_csv(f"{DATA_DIR}/patient_reports.csv")
    
    patients_reports = patients_reports.rename(columns={'label': 'disease'})
    patients_reports['symptom'] = ""
    patients_reports = patients_reports[['disease', 'symptom', 'text']]
    all_separated_symptoms['text'] = ""
    
    merged_symptoms_text = pd.concat([all_separated_symptoms, patients_reports], ignore_index=True)
    merged_symptoms_text['disease'] = merged_symptoms_text['disease'].str.lower().str.strip()
    merged_symptoms_text = merged_symptoms_text.sort_values(by='disease').reset_index(drop=True)
    
    merged_symptoms_text.to_csv(f"{CLEAN_DIR}/merged_symptoms_text.csv", index=False)
    print(f"✓ Merged with patient reports: {len(merged_symptoms_text)} total entries")
    print(f"Sample data:\n{merged_symptoms_text.head(10)}")

def process_train_data():
    """Process training data and merge with existing data"""
    print("Processing training data...")
    merged_symptoms_text = pd.read_csv(f"{CLEAN_DIR}/merged_symptoms_text.csv")
    train01 = pd.read_csv(f"{DATA_DIR}/train-00000-of-00001.csv")
    
    train01 = train01.rename(columns={
        'Source_URL': 'source_url',
        'Disease_Name': 'disease',
        'Symptom_List': 'symptom',
        'Generated_Sentence_From_symptoms': 'text'
    })
    
    train01 = train01.loc[:, ~train01.columns.str.contains('^Unnamed')]
    train01['disease'] = train01['disease'].str.lower().str.strip()
    
    train01 = train01.assign(symptom=train01['symptom'].str.split(' \| ')).explode('symptom')
    train01['symptom'] = train01['symptom'].str.strip()
    
    if 'source_url' not in merged_symptoms_text.columns:
        merged_symptoms_text['source_url'] = None
    
    train01 = train01[['disease', 'symptom', 'text', 'source_url']]
    merged_symptoms_text = merged_symptoms_text[['disease', 'symptom', 'text', 'source_url']]
    
    final = pd.concat([merged_symptoms_text, train01], ignore_index=True)
    final['disease'] = final['disease'].str.lower().str.strip()
    final = final.sort_values(by='disease').reset_index(drop=True)
    
    final.to_csv(f"{CLEAN_DIR}/final.csv", index=False)
    print(f"✓ Processed train data: {len(final)} total entries")
    print(f"Sample data:\n{final.head(10)}")

def process_treatments():
    """Process diseases with treatments data"""
    print("Processing treatments data...")
    diseases_symptoms_with_treatment = pd.read_csv(f"{DATA_DIR}/Diseases_Symptoms.csv")
    diseases_with_treatment = diseases_symptoms_with_treatment.drop('Disease_Code', axis=1)
    diseases_with_treatment = diseases_with_treatment.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    
    diseases_with_treatment = diseases_with_treatment.rename(columns={
        'Name': 'disease',
        'Symptoms': 'symptom',
        'Treatments': 'treatment',
        'Contagious': 'contagious',
        'Chronic': 'chronic'
    })
    
    # Convert to binary: 0 -> non-contagious/non-chronic, 1 -> contagious/chronic
    boolean_columns = ['contagious', 'chronic']
    diseases_with_treatment[boolean_columns] = diseases_with_treatment[boolean_columns].astype(int)
    
    diseases_with_treatment.to_csv(f"{CLEAN_DIR}/diseases_with_treatment.csv", index=False)
    print(f"✓ Processed treatments for {len(diseases_with_treatment)} entries")

def create_final_dataset():
    """Create the final merged dataset"""
    print("Creating final merged dataset...")
    df_1 = pd.read_csv(f"{CLEAN_DIR}/final.csv")
    df_2 = pd.read_csv(f"{CLEAN_DIR}/diseases_with_treatment.csv")
    
    for df in [df_1, df_2]:
        df['disease'] = df['disease'].astype(str).str.strip().str.lower()
        df['symptom'] = df['symptom'].astype(str).str.strip().str.lower()
        df['symptom'] = df['symptom'].str.strip().str.strip('"')
    
    final_dataset = pd.merge(df_1, df_2, on=['disease', 'symptom'], how='outer')
    final_dataset = final_dataset.fillna('')
    final_dataset = final_dataset.sort_values(by='disease')
    
    final_dataset.to_csv(f"{FINAL_DIR}/merged_final_dataset.csv", index=False)
    
    print(f"✓ Final dataset created with {len(final_dataset)} entries")
    print(f"✓ Unique diseases: {final_dataset['disease'].nunique()}")
    print(f"✓ Unique symptoms: {final_dataset['symptom'].nunique()}")
    print(f"\nSample of final dataset:\n{final_dataset.head(15)}")

# Define tasks
create_dirs = PythonOperator(
    task_id='create_directories',
    python_callable=create_directories,
    dag=dag,
)

separate_task1 = PythonOperator(
    task_id='separate_symptoms_from_disease_list',
    python_callable=separate_symptoms_task1,
    dag=dag,
)

separate_task2 = PythonOperator(
    task_id='separate_symptoms_from_diseases',
    python_callable=separate_symptoms_task2,
    dag=dag,
)

merge_symptoms = PythonOperator(
    task_id='merge_separated_symptoms',
    python_callable=merge_separated_symptoms,
    dag=dag,
)

merge_patients = PythonOperator(
    task_id='merge_with_patient_reports',
    python_callable=merge_with_patient_reports,
    dag=dag,
)

process_train = PythonOperator(
    task_id='process_train_data',
    python_callable=process_train_data,
    dag=dag,
)

process_treat = PythonOperator(
    task_id='process_treatments',
    python_callable=process_treatments,
    dag=dag,
)

final_merge = PythonOperator(
    task_id='create_final_dataset',
    python_callable=create_final_dataset,
    dag=dag,
)

# Define task dependencies
# Create directories first
create_dirs >> [separate_task1, separate_task2]

# Both separation tasks must complete before merging
[separate_task1, separate_task2] >> merge_symptoms

# Sequential processing of data transformations
merge_symptoms >> merge_patients >> process_train

# Process treatments can happen after train data is ready
process_train >> process_treat

# Final merge requires both streams to be complete
process_treat >> final_merge
