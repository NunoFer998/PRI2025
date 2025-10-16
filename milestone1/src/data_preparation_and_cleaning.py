import shutil
import polars as pl
import duckdb
from nlp_processor import extract_symptoms
from pathlib import Path

################################
############ SET UP ############
################################

# Set up paths for the datasets
def setup_paths():
    base_path = Path(__file__).parent.parent / 'data'
    paths = {
        'original': base_path / 'original',
        'clean': base_path / 'clean',
        'final': base_path / 'final',
        'prepared': base_path / 'prepared'
    }
    paths['clean'].mkdir(parents=True, exist_ok=True)
    paths['final'].mkdir(parents=True, exist_ok=True)
    return paths


################################
######## DATA CLEANING #########
################################

# Clean the 'clean' directory
def clean_folders(**kwargs):
    print("\nReseting 'clean' directory.\n")
    
    paths = setup_paths()
    if paths['clean'].exists():
        print(f"Removing existing clean folder: {paths['clean']}")
        shutil.rmtree(paths['clean'])
        print(f"Removed existing clean folder")
    paths['clean'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new clean folder: {paths['clean']}")
    return {'clean_folder': str(paths['clean']), 'status': 'cleaned'}

# Cleans the original datasets and saves the result to 'clean' directory
def clean_datasets_task(**kwargs):
    print("\nCleaning datasets.\n")
    
    paths = setup_paths()
    if paths['clean'].exists():
        print(f"Removing existing clean folder: {paths['clean']}")
        shutil.rmtree(paths['clean'])
        print(f"Removed existing clean folder")
    
    paths['clean'].mkdir(parents=True, exist_ok=True)
    print(f"Created new clean folder: {paths['clean']}")
    csv_files = list(paths['original'].glob('*.csv'))
    cleaning_stats = {}
    
    for csv_file in csv_files:
        print(f"Processing file: {csv_file.name}")
        df = pl.read_csv(csv_file)
        initial_rows = df.shape[0]
        df = df.select([c for c in df.columns if df[c].null_count() < df.height])
        df_cleaned = df.drop_nulls()
        after_na_rows = df_cleaned.shape[0]
        
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == pl.Boolean:
                df_cleaned = df_cleaned.with_columns(pl.col(col).cast(pl.Int8).alias(col))
            
        df_cleaned = df_cleaned.unique()
        final_rows = df_cleaned.shape[0]
        df_cleaned = df_cleaned.rename({col: col.strip().lower().replace(' ', '_') for col in df_cleaned.columns})
        cleaned_file_path = paths['clean'] / csv_file.name
        df_cleaned.write_csv(cleaned_file_path)

        
        cleaning_stats[csv_file.name] = {
            'initial_rows': initial_rows,
            'after_na_removal': after_na_rows,
            'final_rows': final_rows,
            'rows_removed': initial_rows - final_rows
        }
        
        print(f"Cleaned file saved: {cleaned_file_path}")
        print(f"   Initial rows: {initial_rows}, After NA removal: {after_na_rows}, Final rows: {final_rows}, Rows removed: {initial_rows - final_rows}")
    return {'cleaning_stats': cleaning_stats, 'status': 'datasets_cleaned'}


################################
####### DATA PREPARATION #######
################################

# Prepare 'disease_symptom_list.csv' from 'clean'
# Renames columns and adds missing ones
def prepare_disease_symptom_list_task(**kwargs):
    print("\nPreparing Disease-Symptom List\n")
    
    paths = setup_paths()
    paths['prepared'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new prepared folder: {paths['prepared']}")
    csv_file = pl.read_csv(paths['clean'] / 'disease_symptom_list.csv')
    csv_file = csv_file.rename({'disease': 'name'})
    csv_file = add_col_to_df(csv_file)
    final_file_path = paths['prepared'] / 'disease_symptom_list.csv'
    csv_file.write_csv(final_file_path)
    
    print(f"Final dataset saved: {final_file_path}")
    return {'final_file': str(final_file_path), 'status': 'disease_symptom_list_prepared'}

# Prepare 'Diseases_Symptoms.csv'
# Remove 'disease_code' and add missing columns
def prepare_disease_symptom_task(**kwargs):
    print("\nPreparing Disease-Symptom List\n")
    
    paths = setup_paths()
    paths['prepared'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new prepared folder: {paths['prepared']}")
    csv_file = pl.read_csv(paths['clean'] / 'Diseases_Symptoms.csv')
    
    if 'disease_code' in csv_file.columns:
        csv_file = csv_file.drop('disease_code')
    
    csv_file = add_col_to_df(csv_file)
    final_file_path = paths['prepared'] / 'Diseases_Symptoms.csv'
    csv_file.write_csv(final_file_path)
    
    print(f"Final dataset saved: {final_file_path}")
    return {'final_file': str(final_file_path), 'status': 'disease_symptom_list_prepared'}

# Prepare 'diseases_with_symptoms.csv'
# Remove 'symptom_count' column, rename and add missing ones 
def prepare_diseases_with_symptoms_task(**kwargs):
    print("\nPreparing Disease-Symptom List\n")
    
    paths = setup_paths()
    paths['prepared'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new prepared folder: {paths['prepared']}")
    csv_file = pl.read_csv(paths['clean'] / 'diseases_with_symptoms.csv')
    csv_file = csv_file.rename({'disease': 'name'})
    
    if 'symptom_count' in csv_file.columns:
        csv_file = csv_file.drop('symptom_count')
    
    csv_file = add_col_to_df(csv_file)
    final_file_path = paths['prepared'] / 'diseases_with_symptoms.csv'
    csv_file.write_csv(final_file_path)
    
    print(f"Final dataset saved: {final_file_path}")
    return {'final_file': str(final_file_path), 'status': 'disease_symptom_list_prepared'}

# Prepare 'patient_reports.csv'
# Rename columns and add missing ones; remove empty or unnamed columns
def prepare_patient_reports_task(**kwargs):
    """
    Task 2: Prepare disease-symptom list from cleaned datasets
    Apply NLP processing to extract symptoms from patient report text
    Save final dataset to final folder
    """
    print("=" * 60)
    print("TASK 2: Preparing Patient Reports")
    print("=" * 60)

    
    paths = setup_paths()
    paths['prepared'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new prepared folder: {paths['prepared']}")
    csv_file = pl.read_csv(paths['clean'] / 'patient_reports.csv')
    csv_file = csv_file.rename({'label': 'name', 'text': 'description'})
    
    if "" in csv_file.columns:
        csv_file = csv_file.drop("")
    
    csv_file = add_col_to_df(csv_file)

    count = 0
    updated_rows = []

    for row in csv_file.iter_rows(named=True):
        print(f"{count} out of 1200", end='\r')
        description = row['description']

        if description and isinstance(description, str) and description.strip():
            symptoms = extract_symptoms(description)
            symptoms_str = ', '.join(symptoms)
            print(f"Extracted symptoms: {symptoms_str}")
        else:
            symptoms_str = ""

        # Save the updated row (don't modify csv_file here)
        updated_rows.append({
            **row,
            'symptoms': symptoms_str
        })

        count += 1

    # After the loop, create a new DataFrame once
    csv_file = pl.DataFrame(updated_rows)

    # Normalize the 'name' column
    if 'name' in csv_file.columns:
        csv_file = csv_file.with_columns(
            pl.col('name').str.strip_chars().str.to_lowercase().str.replace_all(' ', '_').alias('name')
        )
    
    # Clean duplicates and sort
    csv_file = csv_file.unique()
    if 'name' in csv_file.columns:
        csv_file = csv_file.sort('name')
        csv_file = csv_file.with_columns(
            pl.col('name').str.strip_chars().str.to_lowercase().str.replace_all(' ', '_').alias('name')
        )

    final_file_path = paths['prepared'] / 'patient_reports.csv'
    csv_file.write_csv(final_file_path)
    print(f"\n✓ Final dataset saved: {final_file_path}")
    print(f"  Total reports processed: {csv_file.height}")

    return {
        'final_file': str(final_file_path),
        'status': 'patient_reports_prepared'
    }
# Prepare 'train-00000-of-00001.csv'
# Rename columns, replace the '|' separator, add missing columns
def prepare_train_0000_of_0001_task(**kwargs):
    print("\nPreparing train-0000_of_0001\n")
    
    paths = setup_paths()
    paths['prepared'].mkdir(parents=True, exist_ok=True)
    
    print(f"Created new prepared folder: {paths['prepared']}")
    csv_file = pl.read_csv(paths['clean'] / 'train-00000-of-00001.csv')
    csv_file = csv_file.rename({
        'source_url': 'url',
        'disease_name': 'name',
        'symptom_list': 'symptoms',
        'generated_sentence_from_symptoms': 'description'
    })

    # Normalize the 'name' column
    if 'name' in csv_file.columns:
        csv_file = csv_file.with_columns(
            pl.col('name').str.strip_chars().str.to_lowercase().str.replace_all(' ', '_').alias('name')
        )

    if 'symptoms' in csv_file.columns:
        csv_file = csv_file.with_columns(pl.col('symptoms').str.replace_all(r'\|', ', ').alias('symptoms'))
    
    csv_file = add_col_to_df(csv_file)
    final_file_path = paths['prepared'] / 'train-00000-of-00001.csv'
    csv_file.write_csv(final_file_path)
    
    print(f"Final dataset saved: {final_file_path}")
    return {'final_file': str(final_file_path), 'status': 'disease_symptom_list_prepared'}


################################
######## MERGE DATASETS ########
################################

# Merges the prepared datasets
def merge_datasets_task(**kwargs):
    print("\nMerging Datasets (DuckDB)\n")

    paths = setup_paths()
    if paths['final'].exists():
        shutil.rmtree(paths['final'])
    paths['final'].mkdir(parents=True, exist_ok=True)
    
    prepared_files = list(paths['prepared'].glob('*.csv'))
    column_order = ['name', 'symptoms', 'description', 'treatments', 'contagious', 'chronic', 'url']
    
    con = duckdb.connect()
    table_names = []
    
    for file in prepared_files:
        table_name = file.stem.lower().replace('-', '_')
        table_names.append(table_name)
        
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE temp_{table_name} AS
            SELECT * FROM read_csv_auto('{file}', header=True)
        """)
        
        columns_query = con.execute(f"DESCRIBE temp_{table_name}").fetchall()
        available_columns = [col[0] for col in columns_query]
        select_cols = []
        for col in column_order:
            if col in available_columns:
                select_cols.append(f"{col}")
            else:
                select_cols.append(f"NULL as {col}")
        
        con.execute(f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT {', '.join(select_cols)} FROM temp_{table_name}
        """)
        print(f"Registered {file.name} as table {table_name} with columns in order")
    
    union_query = " UNION ALL ".join([f"SELECT * FROM {t}" for t in table_names])
    final_df = con.execute(union_query).pl()
    
    if 'name' in final_df.columns:
        final_df = final_df.with_columns(
            pl.col('name').str.strip_chars().str.to_lowercase().str.replace_all(' ', '_').alias('name')
        )
    final_df = final_df.unique()
    
    if 'name' in final_df.columns:
        final_df = final_df.sort('name')
    
    final_df = final_df.select(column_order)
    final_file_path = paths['final'] / 'merged_disease_symptom_list.csv'
    final_df.write_csv(final_file_path)
    
    print(f"Merged dataset saved: {final_file_path}")
    print(f"  Total rows: {final_df.shape[0]:,}")
    print(f"  Columns: {', '.join(final_df.columns)}")
    con.close()
    
    return {'final_file': str(final_file_path), 'status': 'datasets_merged'}


################################
############# UTILS ############
################################

def create_disease_contagious_chronic_map():
    csv_file = pl.read_csv(setup_paths()['clean'] / 'Diseases_Symptoms.csv')
    disease_map = {}
    for row in csv_file.iter_rows(named=True):
        disease = row['name'].strip().lower().replace(' ', '_')
        contagious = row.get('contagious', None)
        chronic = row.get('chronic', None)
        treatment = row.get('treatments', None)
        disease_map[disease] = {'contagious': contagious, 'chronic': chronic, 'treatments': treatment}
    return disease_map

def add_col_to_df(df):
    columns_to_add = {
        'name': '',
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


################################
############ EXECUTE ###########
################################

if __name__ == "__main__":
    clean_folders()
    clean_datasets_task()
    prepare_disease_symptom_list_task()
    prepare_disease_symptom_task()
    prepare_diseases_with_symptoms_task()
    prepare_patient_reports_task()
    prepare_train_0000_of_0001_task()
    merge_datasets_task()
