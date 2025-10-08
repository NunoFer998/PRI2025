import csv
import json
from collections import defaultdict

input_csv = "data/final/merged_final_dataset.csv"
symptom_search_json = "data/processed/symptom_search.json"
disease_search_json = "data/processed/disease_search.json"

# convert numeric or text to boolean
def convert_to_bool(value):
    value = str(value).strip().lower()
    return value in ["1", "1.0", "true", "yes"]

# remove spaces and quotes
def clean_and_strip(string):
    return str(string).strip().strip('"')

# generate json files
def generate_jsons():
    symptom_dict = defaultdict(list)
    disease_dict = {}

    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            disease = row.get("disease", "").strip().strip('"').lower()
            symptom = row.get("symptom", "").strip().strip('"').lower()
            text = row.get("text", "").strip().strip('"')
            source_url = row.get("source_url", "").strip().strip('"')
            treatment = row.get("treatment", "").strip().strip('"')
            contagious = convert_to_bool(row.get("contagious", "0"))
            chronic = convert_to_bool(row.get("chronic", "0"))

            disease_info = {
                "disease": disease,
                "text": text,
                "source_url": source_url,
                "treatment": treatment,
                "contagious": contagious,
                "chronic": chronic
            }
            symptom_dict[symptom].append(disease_info)

            if disease not in disease_dict:
                disease_dict[disease] = {
                    "symptoms": [],
                    "treatments": set(),
                    "sources": set(),
                    "contagious": contagious,
                    "chronic": chronic
                }

            disease_dict[disease]["symptoms"].append(symptom)
            if treatment:
                disease_dict[disease]["treatments"].add(treatment)
            if source_url:
                disease_dict[disease]["sources"].add(source_url)

    for d in disease_dict.values():
        d["treatments"] = list(d["treatments"])
        d["sources"] = list(d["sources"])

    # symptom search
    with open(symptom_search_json, "w", encoding="utf-8") as f:
        json.dump(symptom_dict, f, indent=4, ensure_ascii=False)

    #disease search
    with open(disease_search_json, "w", encoding="utf-8") as f:
        json.dump(disease_dict, f, indent=4, ensure_ascii=False)

    print(f"Saved symptom search as: {symptom_search_json}")
    print(f"Saved disease search as: {disease_search_json}")


def main():
    input_file = "data/final/merged_final_dataset.csv"
    
    try:
        generate_jsons()
        print("JSONs generated successfully.")
        
    except FileNotFoundError:
        print(f"Error: Could not find {input_file}")
        print("Make sure you are inside the 'milestone1' directory.")


if __name__ == "__main__":
    main()