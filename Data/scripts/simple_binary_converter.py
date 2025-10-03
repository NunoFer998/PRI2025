import pandas as pd
import csv

def convert_binary_to_symptoms_simple(input_file="Final_Augmented_dataset_Diseases_and_Symptoms.csv", 
                                     output_file="diseases_with_symptoms.csv"):
    """
    Simple and fast conversion from binary matrix to disease-symptom pairs.
    """
    print("🔄 Starting conversion...")
    print(f"📁 Reading from: {input_file}")
    print(f"💾 Saving to: {output_file}")
    
    # Read the header first to get symptom names
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        disease_col = header[0]
        symptom_names = header[1:]  # All columns except the first
        
        print(f"✅ Found {len(symptom_names)} symptoms")
        
        # Prepare output file
        with open(output_file, 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.writer(out_f)
            
            # Write header
            writer.writerow(['disease', 'symptoms', 'symptom_count'])
            
            row_count = 0
            
            # Process each row
            for row in reader:
                if len(row) != len(header):
                    continue  # Skip malformed rows
                
                disease = row[0]
                symptom_values = row[1:]
                
                # Find active symptoms (where value is 1)
                active_symptoms = []
                for i, value in enumerate(symptom_values):
                    try:
                        if int(value) == 1:
                            active_symptoms.append(symptom_names[i])
                    except (ValueError, IndexError):
                        continue  # Skip invalid values
                
                # Write row if there are symptoms
                if active_symptoms:
                    symptoms_str = ', '.join(active_symptoms)
                    writer.writerow([disease, symptoms_str, len(active_symptoms)])
                
                row_count += 1
                if row_count % 1000 == 0:
                    print(f"📊 Processed {row_count} rows...")
    
    print(f"✅ Conversion completed! Processed {row_count} rows.")
    print(f"💾 Results saved to: {output_file}")
    
    # Show preview
    print("\n🔍 Preview of results:")
    try:
        preview_df = pd.read_csv(output_file, nrows=5)
        for i, row in preview_df.iterrows():
            print(f"   {i+1}. {row['disease']} ({row['symptom_count']} symptoms)")
            symptoms = row['symptoms'][:80] + "..." if len(row['symptoms']) > 80 else row['symptoms']
            print(f"      {symptoms}")
            print()
    except Exception as e:
        print(f"Could not show preview: {e}")

if __name__ == "__main__":
    convert_binary_to_symptoms_simple()