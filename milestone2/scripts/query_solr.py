#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path

import requests


def fetch_solr_results(query_file, system_file, solr_uri, collection):
    """
    Fetch search results from a Solr instance based on the query parameters.

    Arguments:
    - query_file: Path to the JSON file containing Solr query parameters.
    - solr_uri: URI of the Solr instance (e.g., http://localhost:8983/solr).
    - collection: Solr collection name from which results will be fetched.

    Output:
    - Prints the JSON search results to STDOUT.
    """
    # Load the query parameters from the JSON file
    try:
        # Abre e carrega os ficheiros. system_file agora é do tipo Path.
        query_params = json.loads(query_file.read_text())
        system_params = json.loads(system_file.read_text())
    except FileNotFoundError:
        # CORREÇÃO: Mensagem de erro para STDERR
        print(f"Error: Required query file ({query_file}) or system file ({system_file}) not found.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        # Adicionada gestão de erro para ficheiros JSON mal formatados
        print(f"Error: Invalid JSON format found in configuration files. Details: {e}", file=sys.stderr)
        sys.exit(1)

    # Construct the Solr request URL
    uri = f"{solr_uri}/{collection}/select"

    try:
        params = {
            "query": query_params["query"],
            "fields": query_params["fields"],
            "params": {
                **system_params,
                "start": 0,
                "rows": 20,
                "fl": query_params["fields"], 
            }
        }
        # Send the POST request to Solr
        params = {k: v for k, v in params.items() if v is not None}
        
        # Send the POST request to Solr
        response = requests.post(uri, json=params)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error querying Solr: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch and print the results as JSON
    results = response.json()
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    # Set up argument parsing for the command-line interface
    parser = argparse.ArgumentParser(
        description="Fetch search results from Solr and output them in JSON format."
    )

    # Add arguments for query file, Solr URI, and collection name
    parser.add_argument(
        "--query",
        type=Path,
        required=True,
        help="Path to the JSON file containing the Solr query parameters.",
    )

    parser.add_argument(
        "--system",
        type=Path,
        required=True,
        help="System name (e.g., 'basic' or 'enhanced') to identify the query parameters.",
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
        default="courses",
        help="Name of the Solr collection to query (default: 'courses').",
    )



    # Parse command-line arguments
    args = parser.parse_args()

    # Call the function with parsed arguments
    fetch_solr_results(args.query,args.system, args.uri, args.collection)
