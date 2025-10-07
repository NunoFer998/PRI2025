import pandas as pd
import csv

def convert_binary_to_symptoms_simple(input_file="Final_Augmented_dataset_Diseases_and_Symptoms.csv", output_file="diseases_with_symptoms.csv"):
    print("Starting conversion...")
    print(f"Reading from: {input_file}")
    print(f"Saving to: {output_file}")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        disease_col = header[0]
        symptom_names = header[1:] 
        
        print(f"Found {len(symptom_names)} symptoms")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.writer(out_f)
            
            writer.writerow(['disease', 'symptoms', 'symptom_count'])
            
            row_count = 0
            
            for row in reader:
                if len(row) != len(header):
                    continue 
                
                disease = row[0]
                symptom_values = row[1:]
                
                active_symptoms = []
                for i, value in enumerate(symptom_values):
                    try:
                        if int(value) == 1:
                            active_symptoms.append(symptom_names[i])
                    except (ValueError, IndexError):
                        continue 
                
                if active_symptoms:
                    symptoms_str = ', '.join(active_symptoms)
                    writer.writerow([disease, symptoms_str, len(active_symptoms)])
                
                row_count += 1
                if row_count % 1000 == 0:
                    print(f"Processed {row_count} rows...")
    
    print(f"Conversion completed! Processed {row_count} rows.")
    print(f"Results saved to: {output_file}")
    
    # Show preview
    print("\Preview of results:")
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