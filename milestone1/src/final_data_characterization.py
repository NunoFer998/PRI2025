import pandas as pd

# Load the CSV file
file_path = 'milestone1/data/final/merged_disease_symptom_list.csv'  # Update with your actual file path
df = pd.read_csv(file_path)

# Open a text file to write the analysis
with open("analysis.txt", "w", encoding="utf-8") as f:
    # Basic information
    f.write("=== Dataset Info ===\n")
    df.info(buf=f)  # info can write directly to a buffer
    f.write("\n=== First 5 Rows ===\n")
    f.write(df.head().to_string())
    f.write("\n\n=== Column-wise Summary ===\n")

    # Column-wise characterization
    for col in df.columns:
        f.write(f"\nColumn: {col}\n")
        f.write("-" * 50 + "\n")
        
        # Data type
        f.write(f"Data type: {df[col].dtype}\n")
        
        # Missing values
        missing = df[col].isna().sum()
        f.write(f"Missing values: {missing}\n")
        
        # Unique values
        unique = df[col].nunique()
        f.write(f"Unique values: {unique}\n")
        
        # Numeric summary
        if pd.api.types.is_numeric_dtype(df[col]):
            f.write("Numeric Summary:\n")
            f.write(df[col].describe().to_string() + "\n")
        
        # Top frequent values for strings
        elif pd.api.types.is_object_dtype(df[col]):
            f.write("Top 5 frequent values:\n")
            f.write(df[col].value_counts().head().to_string() + "\n")

    # Symptoms analysis
    if 'symptoms' in df.columns:
        f.write("\n=== Symptoms Analysis ===\n")
        all_symptoms = df['symptoms'].dropna().str.split(',').explode().str.strip()
        f.write(f"Total unique symptoms: {all_symptoms.nunique()}\n")
        f.write("Top 10 most common symptoms:\n")
        f.write(all_symptoms.value_counts().head(10).to_string() + "\n")

print("Analysis has been saved to 'analysis.txt'.")