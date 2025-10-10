"""
Data Preparation and Cleaning Script for Apache Airflow
Transforms and merges multiple disease and symptom datasets
"""

import os
import shutil
import polars as pl
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
        'final': base_path / 'final',
        'prepared': base_path / 'prepared'

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


def clean_datasets_task(**kwargs):
    """
    Task 1: Clean all datasets from original folder - remove NA and duplicates
    Save cleaned datasets to clean folder
    """
    print("=" * 60)
    print("TASK 1: Cleaning Datasets")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Remove and recreate clean folder
    if paths['clean'].exists():
        print(f"Removing existing clean folder: {paths['clean']}")
        shutil.rmtree(paths['clean'])
        print(f"✓ Removed existing clean folder")
    
    paths['clean'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh clean folder: {paths['clean']}")
    
    # Get all CSV files from original folder
    csv_files = list(paths['original'].glob('*.csv'))
    
    cleaning_stats = {}
    
    for csv_file in csv_files:
        print(f"Processing file: {csv_file.name}")
        df = pl.read_csv(csv_file)
        initial_rows = df.shape[0]
        # Drop columns with all NA values
        df = df.select([c for c in df.columns if df[c].null_count() < df.height])

        # Drop rows with any NA values
        df_cleaned = df.drop_nulls()
        after_na_rows = df_cleaned.shape[0]

        # Map boolean columns to binary (True/False)
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == pl.Boolean:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).cast(pl.Int8).alias(col)
                )
        
        # Drop duplicate rows
        df_cleaned = df_cleaned.unique()
        final_rows = df_cleaned.shape[0]
        
        # Normalize column names to lowercase with underscores
        df_cleaned = df_cleaned.rename({col: col.strip().lower().replace(' ', '_') for col in df_cleaned.columns})
        
        # Save cleaned dataset
        cleaned_file_path = paths['clean'] / csv_file.name
        df_cleaned.write_csv(cleaned_file_path)
        
        # Record cleaning stats
        cleaning_stats[csv_file.name] = {
            'initial_rows': initial_rows,
            'after_na_removal': after_na_rows,
            'final_rows': final_rows,
            'rows_removed': initial_rows - final_rows
        }
        
        print(f"✓ Cleaned file saved: {cleaned_file_path}")
        print(f"   Initial rows: {initial_rows}, After NA removal: {after_na_rows}, Final rows: {final_rows}, Rows removed: {initial_rows - final_rows}")
    
    return {
        'cleaning_stats': cleaning_stats,
        'status': 'datasets_cleaned'
    }

def prepare_disease_symptom_list_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing Disease-Symptom List")
    print("=" * 60)
    
    paths = setup_paths()

    paths['prepared'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh prepared folder: {paths['prepared']}")

    csv_file = pl.read_csv(paths['clean'] / 'disease_symptom_list.csv')
    csv_file = csv_file.rename({'disease': 'name'})

    csv_file = add_col_to_df(csv_file)

    final_file_path = paths['prepared'] / 'disease_symptom_list.csv'
    csv_file.write_csv(final_file_path)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }

def prepare_disease_symptom_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing Disease-Symptom List")
    print("=" * 60)
    
    paths = setup_paths()

    paths['prepared'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh prepared folder: {paths['prepared']}")

    csv_file = pl.read_csv(paths['clean'] / 'Diseases_Symptoms.csv')
    # Drop disease_code column if it exists
    if 'disease_code' in csv_file.columns:
        csv_file = csv_file.drop('disease_code')

    csv_file = add_col_to_df(csv_file)

    final_file_path = paths['prepared'] / 'Diseases_Symptoms.csv'
    csv_file.write_csv(final_file_path)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }

def prepare_diseases_with_symptoms_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing Disease-Symptom List")
    print("=" * 60)
    
    paths = setup_paths()

    paths['prepared'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh prepared folder: {paths['prepared']}")

    csv_file = pl.read_csv(paths['clean'] / 'diseases_with_symptoms.csv')

    csv_file = csv_file.rename({'disease': 'name'})
    # Drop symptom_count column if it exists
    if 'symptom_count' in csv_file.columns:
        csv_file = csv_file.drop('symptom_count')

    csv_file = add_col_to_df(csv_file)

    final_file_path = paths['prepared'] / 'diseases_with_symptoms.csv'
    csv_file.write_csv(final_file_path)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }

def prepare_patient_reports_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing Patient Reports")
    print("=" * 60)
    
    paths = setup_paths()

    paths['prepared'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh prepared folder: {paths['prepared']}")

    csv_file = pl.read_csv(paths['clean'] / 'patient_reports.csv')
    csv_file = csv_file.rename({'label': 'name', 'text': 'description'})
    # Drop unnamed:_0 column if it exists
    if 'unnamed:_0' in csv_file.columns:
        csv_file = csv_file.drop('unnamed:_0')
    
    csv_file = add_col_to_df(csv_file)

    final_file_path = paths['prepared'] / 'patient_reports.csv'
    csv_file.write_csv(final_file_path)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }

def prepare_train_0000_of_0001_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing train-0000_of_0001")
    print("=" * 60)
    
    paths = setup_paths()

    paths['prepared'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh prepared folder: {paths['prepared']}")

    csv_file = pl.read_csv(paths['clean'] / 'train-00000-of-00001.csv')
    csv_file = csv_file.rename({
        'source_url': 'url',
        'disease_name': 'name',
        'symptom_list': 'symptoms',
        'generated_sentence_from_symptoms': 'description'
    })

    # Change symptom list from | to ,
    if 'symptoms' in csv_file.columns:
        csv_file = csv_file.with_columns(
            pl.col('symptoms').str.replace_all(r'\|', ', ').alias('symptoms')
        )

    csv_file = add_col_to_df(csv_file)

    final_file_path = paths['prepared'] / 'train-00000-of-00001.csv'
    csv_file.write_csv(final_file_path)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }
    
def merge_datasets_task(**kwargs):
    """
    Task 3: Merge all prepared datasets into a single final dataset
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 3: Merging Datasets")
    print("=" * 60)
    
    paths = setup_paths()
    
    # Remove and recreate final folder
    if paths['final'].exists():
        print(f"Removing existing final folder: {paths['final']}")
        shutil.rmtree(paths['final'])
        print(f"✓ Removed existing final folder")
    
    paths['final'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh final folder: {paths['final']}")
    
    # Read all prepared datasets
    prepared_files = list(paths['prepared'].glob('*.csv'))
    dataframes = []
    
    for file in prepared_files:
        df = pl.read_csv(file)
        
        # Ensure consistent data types for boolean columns
        if 'contagious' in df.columns:
            df = df.with_columns(pl.col('contagious').cast(pl.Utf8))
        if 'chronic' in df.columns:
            df = df.with_columns(pl.col('chronic').cast(pl.Utf8))
        
        dataframes.append(df)
        print(f"Loaded {file.name} with {df.shape[0]} rows and {df.shape[1]} columns")
    
    # Concatenate all dataframes
    final_df = pl.concat(dataframes, how='diagonal')

    # Normalize names to lowercase with underscores
    if 'name' in final_df.columns:
        final_df = final_df.with_columns(
            pl.col('name').str.strip_chars().str.to_lowercase().str.replace_all(' ', '_').alias('name')
        )
    
    # Drop duplicate rows in the final dataframe
    final_df = final_df.unique()

    # Order by name
    if 'name' in final_df.columns:
        final_df = final_df.sort('name')
    
    # Map known chronic and contagious to other values
    disease_map = create_disease_contagious_chronic_map()
    if 'name' in final_df.columns:
        # Create mapping columns as strings first
        final_df = final_df.with_columns([
            pl.col('name').map_elements(lambda x: str(disease_map.get(x, {}).get('contagious', '')), return_dtype=pl.Utf8).alias('contagious_mapped'),
            pl.col('name').map_elements(lambda x: str(disease_map.get(x, {}).get('chronic', '')), return_dtype=pl.Utf8).alias('chronic_mapped'),
            pl.col('name').map_elements(lambda x: str(disease_map.get(x, {}).get('treatments', '')), return_dtype=pl.Utf8).alias('treatments_mapped')
        ])
        
        # Fill contagious, chronic, and treatments with mapped values if they are null or empty
        final_df = final_df.with_columns([
            pl.when(pl.col('contagious').is_null() | (pl.col('contagious') == ''))
              .then(pl.col('contagious_mapped'))
              .otherwise(pl.col('contagious'))
              .alias('contagious'),
            pl.when(pl.col('chronic').is_null() | (pl.col('chronic') == ''))
              .then(pl.col('chronic_mapped'))
              .otherwise(pl.col('chronic'))
              .alias('chronic'),
            pl.when(pl.col('treatments').is_null() | (pl.col('treatments') == ''))
              .then(pl.col('treatments_mapped'))
              .otherwise(pl.col('treatments'))
              .alias('treatments')
        ]).drop(['contagious_mapped', 'chronic_mapped', 'treatments_mapped'])

    # Save the merged final dataset
    final_file_path = paths['final'] / 'merged_disease_symptom_list.csv'
    final_df.write_csv(final_file_path)
    
    print(f"✓ Merged dataset saved: {final_file_path} with {final_df.shape[0]} rows and {final_df.shape[1]} columns")
    
    return {
        'final_file': str(final_file_path),
        'status': 'datasets_merged'
    }

def create_disease_contagious_chronic_map():
    """
    Create a mapping for diseases to their contagious and chronic status
    """
    # Make a map based on the Disease_Symptoms dataset
    csv_file = pl.read_csv(setup_paths()['clean'] / 'Diseases_Symptoms.csv')
    disease_map = {}
    
    # Convert to dictionary format
    for row in csv_file.iter_rows(named=True):
        disease = row['name'].strip().lower().replace(' ', '_')
        contagious = row.get('contagious', None)
        chronic = row.get('chronic', None)
        treatment = row.get('treatments', None)
        disease_map[disease] = {
            'contagious': contagious,
            'chronic': chronic,
            'treatments': treatment
        }
    return disease_map


def add_col_to_df(df):
    """
    Utility function to add a column to a DataFrame if it doesn't exist
    """
    columns_to_add = {
        'name' : '',
        'symptoms': "",
        'description': "",
        'treatments': "",
        'contagious': None,
        'chronic': None,
        'url': '',
    }
    for col_name, default_value in columns_to_add.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(default_value).alias(col_name))
    return df

# start the script with a test run if executed directly
if __name__ == "__main__":
    clean_folders_task()
    clean_datasets_task()
    prepare_disease_symptom_list_task()
    prepare_disease_symptom_task()
    prepare_diseases_with_symptoms_task()
    prepare_patient_reports_task()
    prepare_train_0000_of_0001_task()
    merge_datasets_task()
