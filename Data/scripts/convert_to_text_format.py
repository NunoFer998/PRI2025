import csv
import random

def create_natural_text_from_symptoms(symptoms_list):
    """
    Convert a list of symptoms into natural text descriptions.
    Creates varied sentence structures to make the text more natural.
    """
    if not symptoms_list:
        return "No specific symptoms reported."
    
    # Templates for different sentence structures
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
    
    # Additional connecting phrases for multiple symptoms
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
    
    # Additional descriptive phrases
    intensifiers = [
        "severe", "mild", "persistent", "occasional", "frequent", 
        "intense", "chronic", "acute", "intermittent", "constant"
    ]
    
    # Clean up symptoms list
    symptoms = [s.strip() for s in symptoms_list if s.strip()]
    
    if len(symptoms) == 1:
        # Single symptom
        symptom = symptoms[0]
        # Occasionally add an intensifier
        if random.random() < 0.3:
            intensifier = random.choice(intensifiers)
            symptom = f"{intensifier} {symptom}"
        template = random.choice(templates)
        return template.format(symptoms=symptom)
    
    elif len(symptoms) == 2:
        # Two symptoms
        template = random.choice(templates)
        if random.random() < 0.5:
            symptom_text = f"{symptoms[0]} and {symptoms[1]}"
        else:
            symptom_text = f"{symptoms[0]}, {random.choice(connectors)} {symptoms[1]}"
        return template.format(symptoms=symptom_text)
    
    else:
        # Multiple symptoms - create more complex descriptions
        template = random.choice(templates)

        if random.random() < 0.6:
            # Standard comma-separated list
            symptom_text = ", ".join(symptoms[:-1]) + f", and {symptoms[-1]}"
        else:
            # Use connector phrase
            main_symptom = symptoms[0]
            other_symptoms = ", ".join(symptoms[1:])
            connector = random.choice(connectors)
            symptom_text = f"{main_symptom}, {connector} {other_symptoms}"
        
        return template.format(symptoms=symptom_text)

def convert_symptoms_to_text(input_file="diseases_with_symptoms.csv", 
                           output_file="symptoms_as_text_data.csv"):
    """
    Convert the symptoms CSV to text format similar to Train_data.csv
    """
    print("🔄 Converting symptoms to natural text format...")
    print(f"📁 Input: {input_file}")
    print(f"💾 Output: {output_file}")
    
    # Set random seed for reproducible results (optional)
    random.seed(42)
    
    processed_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        
        # Write header similar to Train_data.csv
        writer.writerow(['', 'label', 'text'])
        
        for row_index, row in enumerate(reader):
            disease = row['disease']
            symptoms_str = row['symptoms']
            
            # Split symptoms into list
            symptoms_list = [s.strip() for s in symptoms_str.split(',') if s.strip()]
            
            # Generate natural text
            text_description = create_natural_text_from_symptoms(symptoms_list)
            
            # Write row with index, disease label, and text
            writer.writerow([row_index, disease, text_description])
            
            processed_count += 1
            
            if processed_count % 5000 == 0:
                print(f"📊 Processed {processed_count} rows...")
    
    print(f"✅ Conversion completed!")
    print(f"📝 Processed {processed_count} records")
    print(f"💾 Results saved to: {output_file}")
    
    # Show preview
    print(f"\n🔍 Preview of generated text:")
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 5:  # Show first 5 examples
                break
            print(f"   {i+1}. Disease: {row['label']}")
            print(f"      Text: {row['text']}")
            print()

def create_sample_preview(input_file="diseases_with_symptoms.csv", num_samples=10):
    """
    Create a small preview to show different text generation examples
    """
    print(f"🔍 Generating {num_samples} sample text variations...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Get first few rows for demonstration
        sample_rows = []
        for i, row in enumerate(reader):
            if i >= num_samples:
                break
            sample_rows.append(row)
    
    print(f"\n📝 Sample text generations:")
    for i, row in enumerate(sample_rows, 1):
        disease = row['disease']
        symptoms_str = row['symptoms']
        symptoms_list = [s.strip() for s in symptoms_str.split(',') if s.strip()]
        
        # Generate 2 different variations for the same symptoms
        text1 = create_natural_text_from_symptoms(symptoms_list)
        text2 = create_natural_text_from_symptoms(symptoms_list)
        
        print(f"\n{i}. Disease: {disease}")
        print(f"   Symptoms: {symptoms_str[:80]}{'...' if len(symptoms_str) > 80 else ''}")
        print(f"   Text 1: {text1}")
        print(f"   Text 2: {text2}")

if __name__ == "__main__":
    print("🚀 Symptom-to-Text Converter")
    print("=" * 50)
    
    # First show some sample generations
    create_sample_preview()
    
    print("\n" + "=" * 50)
    
    # Ask user if they want to proceed with full conversion
    print("Ready to convert the full dataset?")
    response = input("Press Enter to continue or 'q' to quit: ").strip().lower()
    
    if response != 'q':
        convert_symptoms_to_text()
        print("\n🎉 Conversion complete! Your data is now in text format similar to Train_data.csv")
    else:
        print("👋 Conversion cancelled.")