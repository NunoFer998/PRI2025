import pandas as pd
import numpy as np
from tqdm import tqdm
import sys

def convert_binary_to_symptom_names(input_file, output_file, chunk_size=1000):
    """
    Convert binary symptom matrix to disease-symptom name pairs.
    
    Args:
        input_file: Path to the Final_Augmented_dataset_Diseases_and_Symptoms.csv
        output_file: Path for the output CSV file
        chunk_size: Number of rows to process at a time (for memory efficiency)
    """
    
    print("🔄 Converting binary symptom matrix to symptom names...")
    print(f"📁 Input file: {input_file}")
    print(f"💾 Output file: {output_file}")
    
    # First, read just the header to get column names
    print("📋 Reading column headers...")
    header_chunk = pd.read_csv(input_file, nrows=0)
    symptom_columns = header_chunk.columns[1:]  # Skip the first column (diseases)
    
    print(f"✅ Found {len(symptom_columns)} symptom columns")
    print(f"📊 Processing file in chunks of {chunk_size} rows...")
    
    # Prepare output file
    output_data = []
    
    # Process the file in chunks to handle large files
    chunk_count = 0
    total_rows_processed = 0
    
    # Get total number of rows for progress tracking
    print("📏 Counting total rows...")
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for line in f) - 1  # Subtract 1 for header
    
    print(f"📈 Total rows to process: {total_rows}")
    
    # Create progress bar
    with tqdm(total=total_rows, desc="Converting rows", unit="rows") as pbar:
        # Read file in chunks
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            chunk_count += 1
            
            for index, row in chunk.iterrows():
                disease_name = row.iloc[0]  # First column is disease name
                symptom_values = row.iloc[1:]  # Rest are symptom values (0 or 1)
                
                # Find symptoms where value is 1
                active_symptoms = []
                for symptom_name, value in zip(symptom_columns, symptom_values):
                    if value == 1:
                        active_symptoms.append(symptom_name)
                
                # Create output row
                if active_symptoms:  # Only include if there are symptoms
                    output_data.append({
                        'disease': disease_name,
                        'symptoms': ', '.join(active_symptoms),
                        'symptom_count': len(active_symptoms)
                    })
                
                pbar.update(1)
                total_rows_processed += 1
    
    # Convert to DataFrame and save
    print("💾 Saving results to CSV...")
    output_df = pd.DataFrame(output_data)
    output_df.to_csv(output_file, index=False)
    
    print(f"✅ Conversion completed!")
    print(f"📊 Processed {total_rows_processed} rows")
    print(f"📝 Generated {len(output_data)} disease-symptom combinations")
    print(f"💾 Results saved to: {output_file}")
    
    # Show some statistics
    if len(output_data) > 0:
        avg_symptoms = output_df['symptom_count'].mean()
        max_symptoms = output_df['symptom_count'].max()
        min_symptoms = output_df['symptom_count'].min()
        
        print(f"\n📈 Statistics:")
        print(f"   Average symptoms per disease: {avg_symptoms:.2f}")
        print(f"   Maximum symptoms: {max_symptoms}")
        print(f"   Minimum symptoms: {min_symptoms}")
        
        # Show first few examples
        print(f"\n🔍 First 5 examples:")
        for i in range(min(5, len(output_df))):
            disease = output_df.iloc[i]['disease']
            symptoms = output_df.iloc[i]['symptoms']
            count = output_df.iloc[i]['symptom_count']
            print(f"   {i+1}. {disease} ({count} symptoms)")
            print(f"      Symptoms: {symptoms[:100]}{'...' if len(symptoms) > 100 else ''}")
            print()
    
    return output_df

def create_summary_analysis(df, summary_file):
    """
    Create a summary analysis of the converted data.
    """
    print("📊 Creating summary analysis...")
    
    # Disease frequency
    disease_counts = df['disease'].value_counts()
    
    # Symptom frequency
    all_symptoms = []
    for symptoms_str in df['symptoms']:
        symptoms_list = [s.strip() for s in symptoms_str.split(',')]
        all_symptoms.extend(symptoms_list)
    
    symptom_counts = pd.Series(all_symptoms).value_counts()
    
    # Create summary
    summary_data = {
        'total_records': len(df),
        'unique_diseases': df['disease'].nunique(),
        'unique_symptoms': len(symptom_counts),
        'avg_symptoms_per_disease': df['symptom_count'].mean(),
        'most_common_diseases': disease_counts.head(10).to_dict(),
        'most_common_symptoms': symptom_counts.head(20).to_dict()
    }
    
    # Save detailed analysis
    with open(summary_file, 'w') as f:
        f.write("=== SYMPTOM DATASET ANALYSIS ===\n\n")
        f.write(f"Total records: {summary_data['total_records']}\n")
        f.write(f"Unique diseases: {summary_data['unique_diseases']}\n")
        f.write(f"Unique symptoms: {summary_data['unique_symptoms']}\n")
        f.write(f"Average symptoms per disease: {summary_data['avg_symptoms_per_disease']:.2f}\n\n")
        
        f.write("TOP 10 MOST COMMON DISEASES:\n")
        for disease, count in summary_data['most_common_diseases'].items():
            f.write(f"  {disease}: {count} records\n")
        
        f.write("\nTOP 20 MOST COMMON SYMPTOMS:\n")
        for symptom, count in summary_data['most_common_symptoms'].items():
            f.write(f"  {symptom}: {count} occurrences\n")
    
    print(f"📄 Summary analysis saved to: {summary_file}")

def main():
    """
    Main function to run the conversion.
    """
    # File paths
    input_file = "Final_Augmented_dataset_Diseases_and_Symptoms.csv"
    output_file = "converted_diseases_symptoms.csv"
    summary_file = "symptom_analysis_summary.txt"
    
    try:
        # Convert the file
        df = convert_binary_to_symptom_names(input_file, output_file)
        
        # Create summary analysis
        create_summary_analysis(df, summary_file)
        
        print(f"\n🎉 All done! Check these files:")
        print(f"   📄 {output_file} - Converted disease-symptom data")
        print(f"   📄 {summary_file} - Summary analysis")
        
    except FileNotFoundError:
        print(f"❌ Error: Could not find {input_file}")
        print("Make sure the file exists in the current directory.")
    except Exception as e:
        print(f"❌ Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()