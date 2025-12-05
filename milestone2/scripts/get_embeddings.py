import sys
import json
from sentence_transformers import SentenceTransformer

# Load the SentenceTransformer model
model = SentenceTransformer('all-MiniLM-L6-v2')

def get_embedding(text):
    # The model.encode() method already returns a list of floats
    return model.encode(text, convert_to_tensor=False).tolist()

if __name__ == "__main__":
    # Read JSON from STDIN
    data = json.load(sys.stdin)
    counter = 0

    # Update each document in the JSON data
    for document in data:
        counter += 1
        print(f"Processing document {counter}", file=sys.stderr)
        # Extract fields if they exist, otherwise default to empty strings
        name = document.get("name", "")
        symptoms = document.get("symptoms", "")
        description = document.get("description", "")
        treatments = document.get("treatments", "")

        combined_text = name + " " + symptoms + " " + description + " " + treatments
        document["vector"] = get_embedding(combined_text)

    # Output updated JSON to STDOUT
    json.dump(data, sys.stdout, indent=4, ensure_ascii=False)
