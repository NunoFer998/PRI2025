import requests
import json 
import os
from urllib.parse import urlencode
import matplotlib.pyplot as plt
import numpy as np

SOLR_URL = "http://localhost:8983/solr/diseases/select"
SAVE_PATH = os.path.join(os.path.dirname(__file__), "queries")
os.makedirs(SAVE_PATH, exist_ok=True)

def solr_to_trec(solr_response, run_id="run0"):
    """Convert Solr search results to TREC format."""
    trec_lines = []
    try:
        docs = solr_response["response"]["docs"]
        for rank, doc in enumerate(docs, start=1):
            trec_lines.append(f"0 Q0 {doc['id']} {rank} {doc['score']} {run_id}")
    except KeyError:
        print("Error: Invalid Solr response format.")
    return trec_lines

def qrels_to_trec(qrels_list):
    """Convert qrels to TREC format."""
    trec_lines = []
    for doc_id in qrels_list:
        trec_lines.append(f"0 0 {doc_id.strip()} 1")
    return trec_lines

def plot_precision_recall(y_true, y_pred, output_file, query_id, query_type):
    """Generate precision-recall curve and save to PNG."""
    if not y_pred or not y_true:
        print(f"Warning: No predictions or qrels for {query_id}/{query_type}")
        return
    
    precision = []
    recall = []
    relevant_ranks = []
    relevant_count = 0
    
    for i in range(1, len(y_pred) + 1):
        if y_pred[i - 1] in y_true:
            relevant_count += 1
            relevant_ranks.append(relevant_count / i)
        
        precision.append(relevant_count / i)
        recall.append(relevant_count / len(y_true))
    
    map_score = np.sum(relevant_ranks) / len(y_true) if relevant_ranks else 0
    
    recall_levels = np.linspace(0.0, 1.0, 11)
    interpolated_precision = [
        max([p for p, r in zip(precision, recall) if r >= r_level], default=0)
        for r_level in recall_levels
    ]
    
    auc_score = np.trapz(interpolated_precision, recall_levels)
    
    plt.figure()
    plt.plot(
        recall_levels,
        interpolated_precision,
        drawstyle="steps-post",
        label=f"MAP: {map_score:.4f}, AUC: {auc_score:.4f}",
        linewidth=1,
    )
    
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.xlim(0, 1)
    plt.ylim(0, 1)
    plt.legend(loc="lower left", prop={"size": 10})
    plt.title(f"Precision-Recall Curve - {query_id} ({query_type})")
    
    plt.savefig(output_file, format="png", dpi=300)
    plt.close()
    print(f"  → Precision-Recall plot saved to {output_file}")

information_needs = [
    { # query 1
        # What are the symptoms of aase syndrome?
        "id": "q1",
        "basic": {
            "q": "name:aase_ yndrome",
            "wt": "json"
        },
        "enhanced": {
            "q": "aase syndrome",
            "defType": "edismax",
            "qf": "name^5 symptoms^3 treatments^1",
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

    # Check if qrels file exists for this query
    qrels_file = os.path.join(SAVE_PATH, query_id, "qrels.txt")
    qrels_set = set()
    if os.path.exists(qrels_file):
        with open(qrels_file, 'r') as f:
            qrels_set = {line.strip() for line in f if line.strip()}
        print(f"  Loaded {len(qrels_set)} qrels")
        
        # Save qrels in TREC format
        qrels_trec = qrels_to_trec(list(qrels_set))
        with open(os.path.join(SAVE_PATH, query_id, "qrels.trec"), 'w') as f:
            f.write("\n".join(qrels_trec))

    # Basic queries
    basic_path = os.path.join(SAVE_PATH, query_id, "basic")
    os.makedirs(basic_path, exist_ok=True)

    basic_url = f"{SOLR_URL}?{urlencode(info_need['basic'])}"

    with open(os.path.join(basic_path, "url.txt"), "w") as f:
        f.write(basic_url)
        
    try:
        response_basic = requests.get(SOLR_URL, params=info_need["basic"])
        if response_basic.status_code == 200:
            results_json = response_basic.json()
            
            # Save JSON results
            with open(os.path.join(basic_path, "results.json"), "w") as f:
                json.dump(results_json, f, indent=2)
            
            # Convert to TREC format
            trec_lines = solr_to_trec(results_json, f"{query_id}_basic")
            with open(os.path.join(basic_path, "results.trec"), "w") as f:
                f.write("\n".join(trec_lines))
            
            # Generate precision-recall plot if qrels exist
            if qrels_set:
                doc_ids = [doc['id'] for doc in results_json["response"]["docs"]]
                plot_file = os.path.join(basic_path, "precision_recall.png")
                plot_precision_recall(qrels_set, doc_ids, plot_file, query_id, "basic")
            
            print(f"  ✓ Saved results for {query_id}/basic")
        else:
            print(f"  ✗ ERROR on {query_id}/basic: {response_basic.status_code}")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

    # Enhanced queries
    enhanced_path = os.path.join(SAVE_PATH, query_id, "enhanced")
    os.makedirs(enhanced_path, exist_ok=True)

    enhanced_url = f"{SOLR_URL}?{urlencode(info_need['enhanced'])}"

    with open(os.path.join(enhanced_path, "url.txt"), "w") as f:
        f.write(enhanced_url)

    try:
        response_enhanced = requests.get(SOLR_URL, params=info_need['enhanced'])
        if response_enhanced.status_code == 200:
            results_json = response_enhanced.json()
            
            # Save JSON results
            with open(os.path.join(enhanced_path, "results.json"), "w") as f:
                json.dump(results_json, f, indent=2)
            
            # Convert to TREC format
            trec_lines = solr_to_trec(results_json, f"{query_id}_enhanced")
            with open(os.path.join(enhanced_path, "results.trec"), "w") as f:
                f.write("\n".join(trec_lines))
            
            # Generate precision-recall plot if qrels exist
            if qrels_set:
                doc_ids = [doc['id'] for doc in results_json["response"]["docs"]]
                plot_file = os.path.join(enhanced_path, "precision_recall.png")
                plot_precision_recall(qrels_set, doc_ids, plot_file, query_id, "enhanced")
            
            print(f"  ✓ Saved results for {query_id}/enhanced")
        else:
            print(f"  ✗ ERROR on {query_id}/enhanced: {response_enhanced.status_code}")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

    print()

print("All queries complete.")
print(f"\nResults saved in: {SAVE_PATH}")