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
        df = pd.read_csv(csv_file)
        initial_rows = df.shape[0]
        
        # Drop rows with any NA values
        df_cleaned = df.dropna()
        after_na_rows = df_cleaned.shape[0]
        
        # Drop duplicate rows
        df_cleaned = df_cleaned.drop_duplicates()
        final_rows = df_cleaned.shape[0]
        
        # Save cleaned dataset
        cleaned_file_path = paths['clean'] / csv_file.name
        df_cleaned.to_csv(cleaned_file_path, index=False)
        
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
    # Remove and recreate processed folder
    if paths['processed'].exists():
        print(f"Removing existing processed folder: {paths['processed']}")
        shutil.rmtree(paths['processed'])
        print(f"✓ Removed existing processed folder")

    paths['processed'].mkdir(parents=True, exist_ok=True)
    print(f"✓ Created fresh processed folder: {paths['processed']}")

    csv_file = list(paths['clean']['disease_symptom_list'])
    csv_file.rename(columns={'disease': 'name'}, inplace=True)

    add_col_to_df(csv_file)

    final_file_path = paths['processed'] / 'disease_symptom_list.csv'
    csv_file.to_csv(final_file_path, index=False)
    print(f"✓ Final dataset saved: {final_file_path}")

    return {
        'final_file': str(final_file_path),
        'status': 'disease_symptom_list_prepared'
    }


def add_col_to_df(df):
    """
    Utility function to add a column to a DataFrame if it doesn't exist
    """
    columns_to_add = {
        'name' : '',
        'symptoms': "",
        'description': "",
        'treatment': "",
        'contagious': None,
        'chronic': None,
        'url': '',
    }
    for col_name, default_value in columns_to_add.items():
        if col_name not in df.columns:
            df[col_name] = default_value
    return df
    
