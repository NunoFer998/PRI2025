import pandas as pd
import nltk
from nltk import download
from nltk.corpus import wordnet
import os

FILE_PATH = "data/synonyms_diseases.txt"

def synonyms():

    nltk.download('punkt', quiet=True)
    
    nltk.download('averaged_perceptron_tagger', quiet=True)
    nltk.download('wordnet', quiet=True)

    # Use your merged disease dataset
    file_path = 'data/merged_disease_symptom_list.csv'
    dataset = pd.read_csv(file_path)

    # Extract text from symptoms, descriptions, and treatments
    all_texts = []
    
    # Add symptoms
    for symptom in dataset['symptoms'].dropna():
        if isinstance(symptom, str):
            all_texts.append(symptom)
    
    # Add descriptions
    for desc in dataset['description'].dropna():
        if isinstance(desc, str):
            all_texts.append(desc)
    
    # Add treatments
    for treatment in dataset['treatments'].dropna():
        if isinstance(treatment, str):
            all_texts.append(treatment)

    word_dict = {}
    words_checked = set()

    for text in all_texts:
        if not isinstance(text, str):
            continue

        words = nltk.word_tokenize(text)
        # Focus on nouns and adjectives (medical terms are often nouns/adjectives)
        relevant_words = [
            word.lower() for word, pos in nltk.pos_tag(words) 
            if pos in ['NN', 'NNS', 'JJ', 'JJR', 'JJS'] and word.lower() not in words_checked
        ]

        for word in relevant_words:
            synonyms_list = []
            for syn in wordnet.synsets(word):
                for lm in syn.lemmas():
                    if lm.name().lower() != word:
                        synonyms_list.append(lm.name().replace('_', ' '))
            
            if len(synonyms_list) > 0:
                word_dict[word] = set(synonyms_list)
                words_checked.add(word)

    # Write synonyms to file in Solr format
    with open(FILE_PATH, 'w') as f:
        for k, v in word_dict.items():
            matches = list(v - {k})
            if matches:  # Only write if we have synonyms
                list_synonyms = k + ", " + ", ".join(matches[:10])  # Limit to top 10 synonyms
                f.write(list_synonyms + "\n")
    
    print(f"Synonym file created: {FILE_PATH}")
    print(f"Total words with synonyms: {len(word_dict)}")

if __name__ == '__main__':
    if not os.path.exists(FILE_PATH):
        synonyms()
    else:
        print(f"{FILE_PATH} already exists. Delete it to regenerate.")