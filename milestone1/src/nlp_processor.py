from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline

model_name = "d4data/biomedical-ner-all"  # Multi-entity biomedical NER model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForTokenClassification.from_pretrained(model_name)

def merge_subwords(tokens, labels):
    merged = []
    current_word = ""
    for tok, label in zip(tokens, labels):
        if tok.startswith("##"):
            current_word += tok[2:]
        else:
            if current_word:
                merged.append(current_word)
            current_word = tok
    if current_word:
        merged.append(current_word)
    return merged

def extract_symptoms(text):
    ner_pipeline = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple")
    results = ner_pipeline(text)
    symptoms = [r['word'] for r in results if r['entity_group'] in ["Sign_symptom"]]
    merged_symptoms = merge_subwords(symptoms, ["Sign_symptom"] * len(symptoms))
    return merged_symptoms

