#!/usr/bin/env python3
"""
Hybrid Search Script for Solr

Performs hybrid search combining lexical (BM25) and semantic (vector) search
using Solr's boolean query parser. Reads query structure from hybrid.json.

Usage:
    python hybrid_search.py --query queries/query1.json --system queries/systems/hybrid.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests

# Sentence transformers for generating query embeddings
try:
    from sentence_transformers import SentenceTransformer

    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    SentenceTransformer = Any
    print(
        "Warning: sentence-transformers not installed. Semantic search will be disabled.",
        file=sys.stderr,
    )

# Configuration
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # 384 dimensions - matches schema


def get_query_embedding(query: str, model) -> list:
    """Generate embedding for query text."""
    embedding = model.encode(query)
    return embedding.tolist()


def fetch_hybrid_results(
    query_file: Path, system_file: Path, solr_uri: str, collection: str, top_k: int = 75
):
    """
    Fetch hybrid search results from Solr.

    Arguments:
    - query_file: Path to the JSON file containing query text.
    - system_file: Path to the JSON file containing hybrid system parameters.
    - solr_uri: URI of the Solr instance.
    - collection: Solr collection name.
    - top_k: Number of results to return.

    Output:
    - Prints the JSON search results to STDOUT.
    """
    # Load query and system parameters
    try:
        query_params = json.loads(query_file.read_text())
        system_params = json.loads(system_file.read_text())
    except FileNotFoundError:
        print(
            f"Error: Required query file ({query_file}) or system file ({system_file}) not found.",
            file=sys.stderr,
        )
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format. Details: {e}", file=sys.stderr)
        sys.exit(1)

    query_text = query_params.get("query", "")
    fields = query_params.get("fields", "id,name,symptoms,description,treatments,score")

    # Generate query embedding
    vector_str = None
    if EMBEDDINGS_AVAILABLE:
        print("Loading embedding model...", file=sys.stderr)
        model = SentenceTransformer(EMBEDDING_MODEL)
        query_embedding = get_query_embedding(query_text, model)
        vector_str = "[" + ",".join(map(str, query_embedding)) + "]"

    # Construct the Solr request URL
    uri = f"{solr_uri}/{collection}/select"

    # Build params from system config, substituting placeholders
    if EMBEDDINGS_AVAILABLE and vector_str:
        # Full hybrid search with both lexical and semantic
        params = {
            "params": {
                "q": system_params.get("q", "{!bool filter=$Retrieval must=$Ranking}"),
                "Retrieval": system_params.get(
                    "Retrieval", "{!bool should=$LexicalQ should=$SemanticQ}"
                ),
                "Ranking": system_params.get(
                    "Ranking",
                    "{!func}product(query($normLexical), query($normSemantic))",
                ),
                "normLexical": system_params.get(
                    "normLexical", "{!func}scale(query($LexicalQ), 0, 1)"
                ),
                "normSemantic": system_params.get(
                    "normSemantic", "{!func}scale(query($SemanticQ), 0, 1)"
                ),
                # Substitute $QUERY placeholder
                "LexicalQ": system_params.get(
                    "LexicalQ",
                    "{!edismax qf='name^10 symptoms^5 treatments^20 description^30' q.op=OR}$QUERY",
                ).replace("$QUERY", query_text),
                # Substitute $VECTOR and $TOP_K placeholders
                "SemanticQ": system_params.get(
                    "SemanticQ", "{!knn f=vector topK=$TOP_K}$VECTOR"
                )
                .replace("$VECTOR", vector_str)
                .replace("$TOP_K", str(top_k * 2)),
                "fl": fields,
                "rows": top_k,
                "wt": "json",
            }
        }
    else:
        # Fallback: lexical-only search
        params = {
            "params": {
                "q": query_text,
                "defType": "edismax",
                "qf": "name^10 symptoms^5 treatments^20 description^30",
                "q.op": "OR",
                "fl": fields,
                "rows": top_k,
                "wt": "json",
            }
        }

    print(f"Query: '{query_text}'", file=sys.stderr)
    print(
        f"Semantic search: {'Enabled' if EMBEDDINGS_AVAILABLE else 'Disabled'}",
        file=sys.stderr,
    )

    try:
        response = requests.post(uri, json=params)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error querying Solr: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch and print results as JSON
    results = response.json()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch hybrid search results from Solr (lexical + semantic)."
    )

    parser.add_argument(
        "--query",
        type=Path,
        required=True,
        help="Path to the JSON file containing the query text.",
    )

    parser.add_argument(
        "--system",
        type=Path,
        required=True,
        help="Path to the hybrid system JSON file (e.g., queries/systems/hybrid.json).",
    )

    parser.add_argument(
        "--uri",
        type=str,
        default="http://localhost:8983/solr",
        help="The URI of the Solr instance (default: http://localhost:8983/solr).",
    )

    parser.add_argument(
        "--collection",
        type=str,
        default="diseases",
        help="Name of the Solr collection to query (default: 'diseases').",
    )

    parser.add_argument(
        "--top-k",
        type=int,
        default=75,
        help="Number of results to return (default: 75).",
    )

    args = parser.parse_args()

    fetch_hybrid_results(args.query, args.system, args.uri, args.collection, args.top_k)
