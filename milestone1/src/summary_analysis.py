import pandas as pd
import matplotlib.pyplot as plt
import os

def parse_symptoms(cell):
    if pd.isna(cell):
        return []
    cell = str(cell).strip()
    if cell == "":
        return []
    parts = [p.strip().lower() for p in cell.split(',')]
    return [p for p in parts if p]

def create_summary_analysis(df, summary_file, plots_dir):
    all_symptoms = []
    for s in df['symptoms']:
        all_symptoms.extend(parse_symptoms(s))
    symptom_counts_total = pd.Series(all_symptoms).value_counts()

    def unique_symptoms_for_series(series):
        union_set = set()
        for cell in series:
            union_set.update(parse_symptoms(cell))
        return union_set

    disease_symptom_sets = df.groupby('name')['symptoms'].apply(unique_symptoms_for_series)
    unique_symptom_counts = disease_symptom_sets.apply(len)  # number of unique symptoms per disease

    # Total symptom mentions per disease (count repeats)
    def total_mentions_for_series(series):
        total = 0
        for cell in series:
            total += len(parse_symptoms(cell))
        return total

    total_mention_counts = df.groupby('name')['symptoms'].apply(total_mentions_for_series)

    summary_data = {
        'total_records': len(df),
        'unique_diseases': df['name'].nunique(),
        'unique_symptoms_global': len(symptom_counts_total),
        'avg_unique_symptoms_per_disease': unique_symptom_counts.mean() if len(unique_symptom_counts) > 0 else 0.0,
        'avg_total_symptom_mentions_per_disease': total_mention_counts.mean() if len(total_mention_counts) > 0 else 0.0,
        'most_common_diseases': df['name'].value_counts().head(10).to_dict(),
        'most_common_symptoms_global': symptom_counts_total.head(20).to_dict()
    }

    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=== SYMPTOM DATASET ANALYSIS ===\n\n")
        f.write(f"Total records: {summary_data['total_records']}\n")
        f.write(f"Unique diseases: {summary_data['unique_diseases']}\n")
        f.write(f"Unique symptoms (global): {summary_data['unique_symptoms_global']}\n")
        f.write(f"Average UNIQUE symptoms per disease: {summary_data['avg_unique_symptoms_per_disease']:.2f}\n")
        f.write(f"Average TOTAL symptom mentions per disease: {summary_data['avg_total_symptom_mentions_per_disease']:.2f}\n\n")

        f.write("TOP 10 MOST COMMON DISEASES (by record count):\n")
        for disease, count in summary_data['most_common_diseases'].items():
            f.write(f"  {disease}: {count} records\n")

        f.write("\nTOP 20 MOST COMMON SYMPTOMS (global counts of mentions):\n")
        for symptom, count in summary_data['most_common_symptoms_global'].items():
            f.write(f"  {symptom}: {count} mentions\n")

    print(f"Summary analysis saved to: {summary_file}")
    return symptom_counts_total, summary_data, unique_symptom_counts, total_mention_counts

if __name__ == "__main__":
    path = "data/final"
    plots_dir = os.path.join(path, "plots")

    df = pd.read_csv("data/final/merged_disease_symptom_list.csv")
    create_summary_analysis(df, "data/final/analysis.txt", plots_dir)