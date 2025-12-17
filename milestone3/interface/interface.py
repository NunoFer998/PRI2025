import json
import os
import sys
from pathlib import Path
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.query_solr import query_solr
from scripts import query_embedding

app = Flask(__name__)
CORS(app)

# Base path for system configuration files
SYSTEMS_DIR = Path(__file__).parent.parent / "queries" / "systems"
def clean_text(text):
    if not text:
        return "Unknown"
    if isinstance(text, list): 
        text = text[0] 
    return str(text).replace("_", " ").title()

app.jinja_env.filters['clean_text'] = clean_text

# Available search systems
AVAILABLE_SYSTEMS = {
    "basic": SYSTEMS_DIR / "basic.json",
    "enhanced": SYSTEMS_DIR / "enhanced.json",
    "treatment": SYSTEMS_DIR / "treatmentSort.json",
    "hybrid": SYSTEMS_DIR / "hybrid.json",
}


def load_system_config(system_name):
    """Load a system configuration file."""
    if system_name not in AVAILABLE_SYSTEMS:
        return None
    system_file = AVAILABLE_SYSTEMS[system_name]
    if system_file.exists():
        return json.loads(system_file.read_text())
    return None


@app.route("/")
def home():
    return render_template("homepage.html")

@app.route("/details/<path:id>")
def details(id):
    solr_url = "http://localhost:8983/solr/diseases/select"

    params = {
        "q": f'id:"{id}"', 
        "wt": "json",
        "rows": 1
    }

    try:
        response = requests.get(solr_url, params=params)
        data = response.json()
        docs = data.get("response", {}).get("docs", [])
        
        if not docs:
            return "Document not found", 404
            
        return render_template("details.html", doc=docs[0])
        
    except Exception as e:
        return f"Error: {str(e)}", 500


@app.route("/api/search", methods=["GET"])
def search_solr():
    keyword = request.args.get("q")
    mode = request.args.get("mode", "basic")

    if not keyword:
        return jsonify({"error": 'Query parameter "q" is required'}), 400

    top_k = 20

    if mode == "hybrid":
        try:
            vector_str = query_embedding.text_to_embedding(keyword)
            hybrid_config = load_system_config("hybrid")
            
            if not hybrid_config:
                return jsonify({"error": "Hybrid system configuration not found"}), 500

            # Build hybrid search params with substituted values
            params = {
                "q": hybrid_config.get("q", "{!bool filter=$Retrieval must=$Ranking}"),
                "Retrieval": hybrid_config.get(
                    "Retrieval", "{!bool should=$LexicalQ should=$SemanticQ}"
                ),
                "Ranking": hybrid_config.get(
                    "Ranking",
                    "{!func}product(query($normLexical), query($normSemantic))",
                ),
                "normLexical": hybrid_config.get(
                    "normLexical", "{!func}scale(query($LexicalQ), 0, 1)"
                ),
                "normSemantic": hybrid_config.get(
                    "normSemantic", "{!func}scale(query($SemanticQ), 0, 1)"
                ),
                "LexicalQ": hybrid_config.get(
                    "LexicalQ",
                    "{!edismax qf='name^10 symptoms^5 treatments^20 description^30' q.op=OR}$QUERY",
                ).replace("$QUERY", keyword),
                "SemanticQ": hybrid_config.get(
                    "SemanticQ", "{!knn f=vector topK=$TOP_K}$VECTOR"
                )
                .replace("$VECTOR", vector_str)
                .replace("$TOP_K", str(top_k * 2)),
                "fl": "id,name,symptoms,description,treatments,score",
                "rows": top_k,
                "wt": "json",
            }
            
            # Send request directly for hybrid mode (needs special handling)
            import requests
            solr_url = "http://localhost:8983/solr/diseases/select"
            response = requests.post(solr_url, data=params)
            if response.status_code != 200:
                return (
                    jsonify({"error": "Solr Error", "solr_message": response.text}),
                    response.status_code,
                )
            result = response.json()
            result["debug_mode_used"] = mode
            return jsonify(result)
            
        except Exception as e:
            return jsonify({"error": f"Hybrid search failed: {str(e)}"}), 500

    elif mode == "semantic":
        try:
            vector_str = query_embedding.text_to_embedding(keyword)
            
            import requests
            solr_url = "http://localhost:8983/solr/diseases/select"
            params = {
                "q": f"{{!knn f=vector topK50}}{vector_str}",
                "fl": "id,name,symptoms,description,treatments,score",
                "rows": 50,
                "wt": "json",
            }
            response = requests.post(solr_url, data=params)
            if response.status_code != 200:
                return (
                    jsonify({"error": "Solr Error", "solr_message": response.text}),
                    response.status_code,
                )
            result = response.json()
            result["debug_mode_used"] = mode
            return jsonify(result)

        except Exception as e:
            return jsonify({"error": f"Embedding failed: {str(e)}"}), 500

    else:
        # Use query_solr for standard modes (basic, enhanced, treatment)
        system_name = mode if mode in AVAILABLE_SYSTEMS else "basic"
        system_file = AVAILABLE_SYSTEMS[system_name]
        
        try:
            result = query_solr(
                query_text=keyword,
                system_file=system_file,
                solr_uri="http://localhost:8983/solr",
                collection="diseases",
                rows=top_k
            )
            result["debug_mode_used"] = mode
            return jsonify(result)
            
        except FileNotFoundError as e:
            return jsonify({"error": f"System configuration not found: {str(e)}"}), 500
        except ConnectionError as e:
            return jsonify({"error": f"Solr connection error: {str(e)}"}), 500
        except Exception as e:
            print(f"CRITICAL ERROR: {e}")
            return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(port=5000, debug=True)
