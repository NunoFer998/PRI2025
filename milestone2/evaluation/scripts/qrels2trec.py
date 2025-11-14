#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path


def qrels_to_trec(qrels: list) -> None:
    """
    Converts qrels (query relevance judgments) to TREC evaluation format.

    Arguments:
    - qrels: A list of qrel lines (document IDs).
    """
    for line in qrels:
        doc_id = line.strip()
        if doc_id:  # skip empty lines
            print(f"0 0 {doc_id} 1")


if __name__ == "__main__":
    """
    Read qrels from file or directory and output them in TREC format.
    """
    parser = argparse.ArgumentParser(description='Convert qrels to TREC format')
    parser.add_argument('--qrels', required=True, help='Path to qrels file or directory')
    args = parser.parse_args()
    
    qrels_path = Path(args.qrels)
    
    # If it's a directory, read all files in it
    if qrels_path.is_dir():
        for qrel_file in sorted(qrels_path.glob('*')):
            if qrel_file.is_file():
                with open(qrel_file, 'r') as f:
                    qrels_to_trec(f.readlines())
    # If it's a file, read it directly
    elif qrels_path.is_file():
        with open(qrels_path, 'r') as f:
            qrels_to_trec(f.readlines())
    else:
        print(f"Error: {args.qrels} not found", file=sys.stderr)
        sys.exit(1)