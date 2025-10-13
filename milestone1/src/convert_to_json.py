import csv
import json
from collections import defaultdict

input_csv = "data/final/merged_disease_symptom_list.csv"
symptom_search_json = "data/processed/symptom_search.json"
disease_search_json = "data/processed/disease_search.json"

def convert_to_bool(value):
    value = str(value).strip().lower()
    return value in ["1", "1.0", "true", "yes"]

def clean_and_strip(string):
    return str(string).strip().strip('"')

def generate_jsons():
    symptom_dict = defaultdict(dict)
    disease_dict = {}

    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            disease = clean_and_strip(row.get("name", "")).lower()
            symptoms = clean_and_strip(row.get("symptoms", "")).lower()
            description = clean_and_strip(row.get("description", ""))
            treatments = clean_and_strip(row.get("treatments", ""))
            contagious = convert_to_bool(row.get("contagious", "0"))
            chronic = convert_to_bool(row.get("chronic", "0"))
            source_url = clean_and_strip(row.get("url", ""))

            symptom_list = [s.strip() for s in symptoms.split(",") if s.strip()]

            disease_info = {
                "disease": disease,
                "text": description,
                "source_url": source_url,
                "treatment": treatments,
                "contagious": contagious,
                "chronic": chronic
            }

            for symptom in symptom_list:
                symptom_key = symptom.strip().lower()
                if disease not in symptom_dict[symptom_key]:
                    symptom_dict[symptom_key][disease] = disease_info
                else:
                    # no data loss if any of the other rows have data
                    existing = symptom_dict[symptom_key][disease]
                    if not existing["text"] and description:
                        existing["text"] = description
                    if not existing["source_url"] and source_url:
                        existing["source_url"] = source_url
                    if not existing["treatment"] and treatments:
                        existing["treatment"] = treatments

            # build disease_dict as before
            if disease not in disease_dict:
                disease_dict[disease] = {
                    "symptoms": [],
                    "treatments": set(),
                    "sources": set(),
                    "contagious": contagious,
                    "chronic": chronic
                }

            disease_dict[disease]["symptoms"].extend(symptom_list)
            if treatments:
                disease_dict[disease]["treatments"].add(treatments)
            if source_url:
                disease_dict[disease]["sources"].add(source_url)

    # finalize disease_dict
    for d in disease_dict.values():
        d["symptoms"] = sorted(list(set(d["symptoms"])))
        d["treatments"] = list(d["treatments"])
        d["sources"] = list(d["sources"])

    final_symptom_dict = {k: list(v.values()) for k, v in symptom_dict.items()}

    # symptom-based search
    with open(symptom_search_json, "w", encoding="utf-8") as f:
        json.dump(final_symptom_dict, f, indent=4, ensure_ascii=False)

    # disease-based search
    with open(disease_search_json, "w", encoding="utf-8") as f:
        json.dump(disease_dict, f, indent=4, ensure_ascii=False)

    print(f"Saved symptom search as: {symptom_search_json}")
    print(f"Saved disease search as: {disease_search_json}")

def main():
    try:
        generate_jsons()
        print("JSON files generated successfully.")
    except FileNotFoundError:
        print(f"Error: Could not find {input_csv}")
        print("Make sure you are inside the correct directory.")

if __name__ == "__main__":
    main()
