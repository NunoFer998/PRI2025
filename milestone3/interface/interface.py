import json
import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
from scripts import query_embedding

app = Flask(__name__)
CORS(app)


def load_systems(filename):
    with open(filename, "r") as f:
        return json.load(f)


search_systems = {
    "basic": load_systems("./systems/basic.json"),
    "enhanced": load_systems("./systems/enhanced.json"),
    "treatment": load_systems("./systems/treatmentSort.json"),
    "hybrid": load_systems("./systems/hybrid.json"),
}


@app.route("/")
def home():
    return render_template("interface.html")


@app.route("/api/search", methods=["GET"])
def search_solr():
    keyword = request.args.get("q")
    mode = request.args.get("mode", "basic")

    if not keyword:
        return jsonify({"error": 'Query parameter "q" is required'}), 400

    params = {}
    top_k = 10

    if mode == "hybrid":
        try:
            vector_str = query_embedding.text_to_embedding(keyword)
            hybrid_config = search_systems["hybrid"]

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
        except Exception as e:
            return jsonify({"error": f"Hybrid search failed: {str(e)}"}), 500

    elif mode == "semantic":
        try:
            vector_str = query_embedding.text_to_embedding(keyword)

            params["q"] = f"{{!knn f=vector topK=10}}{vector_str}"
            params["fl"] = "id,name,symptoms,description,treatments,score"
            params["rows"] = 10
            params["wt"] = "json"

        except Exception as e:
            return jsonify({"error": f"Embedding failed: {str(e)}"}), 500

    elif mode in search_systems:
        params = search_systems[mode].copy()
        params["q"] = keyword
        if "rows" not in params:
            params["rows"] = 10
    else:
        params = search_systems["basic"].copy()
        params["q"] = keyword
        params["rows"] = 10

    solr_url = "http://localhost:8983/solr/diseases/select"

    try:
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
        print(f"CRITICAL ERROR: {e}")
        return (jsonify({"error": str(e)}),)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
