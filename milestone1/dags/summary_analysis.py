import pandas as pd
import sys
import os

def create_summary_analysis(df, summary_file):

    disease_counts = df['name'].value_counts()
    all_symptoms = []
    for s in df['symptoms']:
        if pd.notna(s) and s.strip() != "":
            symptoms_list = [sym.strip() for sym in s.split(',')]
            all_symptoms.extend(symptoms_list)

    symptom_counts = pd.Series(all_symptoms).value_counts()

    symptoms_per_disease = df.groupby('name')['symptoms'].count()

    summary_data = {
        'total_records': len(df),
        'unique_diseases': df['name'].nunique(),
        'unique_symptoms': len(symptom_counts),
        'avg_symptoms_per_disease': symptoms_per_disease.mean(),
        'most_common_diseases': disease_counts.head(10).to_dict(),
        'most_common_symptoms': symptom_counts.head(20).to_dict()
    }

    with open(summary_file, 'w', encoding='utf-8') as f:
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

    print(f"Summary analysis saved to: {summary_file}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python summary_analysis.py <input_csv> <output_txt>")
        sys.exit(1)

    path = "data/final"
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    input_csv = os.path.join(path, input_file)
    output_txt = os.path.join(path, output_file)

    if not os.path.isfile(input_csv):
        print(f"Error: File '{input_csv}' does not exist.")
        print("Make sure the file is inside 'data/final'.")
        sys.exit(1)

    df = pd.read_csv(input_csv)
    create_summary_analysis(df, output_txt)