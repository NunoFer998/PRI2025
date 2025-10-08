import csv
import random
from pathlib import Path
import argparse
import sys


def create_natural_text_from_symptoms(symptoms_list):
    if not symptoms_list:
        return "No specific symptoms reported."
    
    templates = [
        "I have been experiencing {symptoms}.",
        "I am suffering from {symptoms}.",
        "I have been having {symptoms}.",
        "I've been dealing with {symptoms}.",
        "I have noticed {symptoms}.",
        "I am experiencing {symptoms}.",
        "I have been troubled by {symptoms}.",
        "I've had {symptoms}.",
        "I am currently experiencing {symptoms}.",
        "I have been showing signs of {symptoms}.",
    ]
    
    connectors = [
        "along with",
        "as well as",
        "in addition to",
        "together with",
        "combined with",
        "accompanied by",
        "and also",
        "plus",
    ]
    
    intensifiers = [
        "severe", "mild", "persistent", "occasional", "frequent", 
        "intense", "chronic", "acute", "intermittent", "constant"
    ]
    
    symptoms = [s.strip() for s in symptoms_list if s.strip()]
    
    if len(symptoms) == 1:
        symptom = symptoms[0]
        if random.random() < 0.3:
            intensifier = random.choice(intensifiers)
            symptom = f"{intensifier} {symptom}"
        template = random.choice(templates)
        return template.format(symptoms=symptom)
    
    elif len(symptoms) == 2:
        template = random.choice(templates)
        if random.random() < 0.5:
            symptom_text = f"{symptoms[0]} and {symptoms[1]}"
        else:
            symptom_text = f"{symptoms[0]}, {random.choice(connectors)} {symptoms[1]}"
        return template.format(symptoms=symptom_text)
    
    else:
        template = random.choice(templates)

        if random.random() < 0.6:
            symptom_text = ", ".join(symptoms[:-1]) + f", and {symptoms[-1]}"
        else:
            main_symptom = symptoms[0]
            other_symptoms = ", ".join(symptoms[1:])
            connector = random.choice(connectors)
            symptom_text = f"{main_symptom}, {connector} {other_symptoms}"
        
        return template.format(symptoms=symptom_text)

def convert_symptoms_to_text(input_file: Path, output_file: Path):
    print("Converting symptoms to natural text format...")
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    random.seed(42)
    
    processed_count = 0
    
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with input_file.open('r', encoding='utf-8') as infile, \
         output_file.open('w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        
        writer.writerow(['', 'label', 'text'])
        
        for row_index, row in enumerate(reader):
            disease = row['disease']
            symptoms_str = row['symptoms']
            
            symptoms_list = [s.strip() for s in symptoms_str.split(',') if s.strip()]
            text_description = create_natural_text_from_symptoms(symptoms_list)
            writer.writerow([row_index, disease, text_description])
            
            processed_count += 1
            
    print(f"Conversion completed!")
    print(f"Processed {processed_count} records")
    print(f"Results saved to: {output_file}")
    
if __name__ == "__main__":
    print("Symptom-to-Text Converter")
    print("=" * 50)
    repo_root = Path(__file__).resolve().parent.parent
    default_input = repo_root / 'data' / 'original' / 'diseases_with_symptoms.csv'
    default_output = repo_root / 'data' / 'clean' / 'symptoms_as_text_data.csv'

    parser = argparse.ArgumentParser(description='Convert disease-symptom CSV to natural text examples')
    parser.add_argument('--input', '-i', default=str(default_input), help='Path to diseases_with_symptoms.csv')
    parser.add_argument('--output', '-o', default=str(default_output), help='Path to write generated text CSV')
    args = parser.parse_args()

    print("\n" + "=" * 50)

    print("Convert the full dataset?")
    try:
        response = input("Press Enter to continue or 'q' to quit: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print('\nNo input detected, exiting.')
        sys.exit(0)

    if response != 'q':
        convert_symptoms_to_text(input_file=Path(args.input), output_file=Path(args.output))
        print("\nConversion complete.")
    else:
        print("Conversion cancelled.")