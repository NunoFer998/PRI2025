import requests
import json 
import os
from urllib.parse import urlencode

SOLR_URL = "http://localhost:8983/solr/diseases/select"
SAVE_PATH = os.path.join(os.path.dirname(__file__), "queries")
os.makedirs(SAVE_PATH, exist_ok=True)

information_needs = [
    { # query 1
        # What are the symptoms of aase syndrome?
        "id": "q1",
        "basic": {
            "q": "name:aase_syndrome",
            "wt": "json"
        },
        "enhanced": {
            "q": "aase syndrome",
            "defType": "edismax",
            "qf": "name^5 symptoms^3 description^1 treatments^1",
            "q.op": "AND",
            "wt": "json"
        }
    },

    { # query 2
        # What diseases can cause headache and fatigue?
        "id": "q2",
        "basic": {
            "q": "symptoms:(headache AND fatigue)",
            "wt": "json"
        },
        "enhanced": {
            "q": "headache fatigue",
            "defType": "edismax",
            "q.op": "AND",
            "qf": "name^5 symptoms^3 description^1 treatments^1",
            "wt": "json"
        }
    },

    { # query 3
        # What are the common treatments for Multiple Sclerosis?
        "id": "q3",
        "basic": {
            "q": "name: multiple_sclerosis",
            "wt": "json"
        },
        "enhanced": {
            "q": "treatments multiple sclerosis",
            "defType": "edismax",
            "qf": "name^5 treatments^4 description^1",
            "wt": "json"
        }
    },

    { #query 4
        # Find all chronic diseases
        "id": "q4",
        "basic": {
            "q": "*:*",
            "fq": "chronic:1",
            "wt": "json"
        },
        "enhanced": {
            "q": "*:*",
            "fq": "chronic:1",
            "wt": "json"
        }
    },

    { # query 5
        # Information on migraines
        "id": "q5",
        "basic": {
            "q": "symptoms: migraines OR description:migraines",
            "wt": "json"
        },
        "enhanced": {
            "q": "migraines",
            "defType": "edismax",
            "qf": "name^5 symptoms^3 description^1",
            "wt": "json"
        }
    },
]

for info_need in information_needs:
    query_id = info_need["id"]
    print(f" --- Processing query: {query_id} --- ")

    # Basic queries
    basic_path = os.path.join(SAVE_PATH, query_id, "basic")
    os.makedirs(basic_path, exist_ok=True)

    basic_url = f"{SOLR_URL}?{urlencode(info_need['basic'])}"

    with open(os.path.join(basic_path, "url.txt"), "w") as f:
        f.write(basic_url)
        
    try:
        response_basic = requests.get(SOLR_URL, params=info_need["basic"])
        if response_basic.status_code == 200:
            with open(os.path.join(basic_path, "results.json"), "w") as f:
                json.dump(response_basic.json(), f, indent=2)
            print(f"Saved results for {query_id}/basic")
        else:
            print(f"ERROR on {query_id}/basic: {response_basic.status_code}")
    except Exception as e:
        print(f"ERROR: {e}")

    # Enhanced queries
    enhanced_path = os.path.join(SAVE_PATH, query_id, "enhanced")
    os.makedirs(enhanced_path, exist_ok=True)

    enhanced_url = f"{SOLR_URL}?{urlencode(info_need['enhanced'])}"

    with open(os.path.join(enhanced_path, "url.txt"), "w") as f:
        f.write(enhanced_url)

    try:
        response_enhanced = requests.get(SOLR_URL, params=info_need['enhanced'])
        if response_enhanced.status_code == 200:
            with open(os.path.join(enhanced_path, "results.json"), "w") as f:
                json.dump(response_enhanced.json(), f, indent=2)
            print(f"Saved results for {query_id}/enhanced")
        else:
            print(f"ERROR on {query_id}/enhanced: {response_enhanced.status_code}")
    except Exception as e:
        print(f"ERROR: {e}")

    print()

print("All queries complete.")