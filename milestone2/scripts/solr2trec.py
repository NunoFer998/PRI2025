#!/usr/bin/env python3

import argparse
import json
import sys


def solr_to_trec(solr_response, qid, run_id="run0"):
    """
    Converts Solr search results to TREC format and writes the results to STDOUT.

    Format:
    qid     iter    docno       rank    sim     run_id
    1       Q0      M.EIC028    1       0.80    run0

    Arguments:
    - solr_response: Dictionary containing Solr response with document IDs and scores.
    - qid: The Query ID for this result set.
    - run_id: Identifier for the experiment or system (default: run0).

    Output:
    - Writes the converted results to STDOUT.
    """
    try:
        # Extract the document results from the Solr response
        docs = solr_response["response"]["docs"]

        try:
            qid_int = int(qid)
        except ValueError:
            print(f"Error: QID '{qid}' is not a valid integer.", file=sys.stderr)
            sys.exit(1)

        # Enumerate through the results and write them in TREC format
        for rank, doc in enumerate(docs, start=1):
            print(f"{qid_int} Q0 {doc['id']} {rank} {doc['score']} {run_id}")

    except KeyError:
        print("Error: Invalid Solr response format. 'docs' key not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Set up argument parsing for command-line interface
    parser = argparse.ArgumentParser(description="Convert Solr results to TREC format.")

    # Add a REQUIRED argument for the Query ID
    parser.add_argument(
        "--qid",
        type=str,
        required=True,
        help="The Query ID (qid) for this result set.",
    )

    # Add argument for optional run ID
    parser.add_argument(
        "--run-id",
        type=str,
        default="run0",
        help="Experiment or system identifier (default: run0).",
    )

    # Parse command-line arguments
    args = parser.parse_args()

    # Load Solr response from STDIN
    try:
        solr_response = json.load(sys.stdin)
    except json.JSONDecodeError:
        print("Error: Invalid JSON input from STDIN.", file=sys.stderr)
        sys.exit(1)


    # Convert Solr results to TREC format and write to STDOUT
    solr_to_trec(solr_response, args.qid, args.run_id)