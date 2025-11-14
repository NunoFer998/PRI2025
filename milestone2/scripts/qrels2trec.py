#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path


def qrels_to_trec(qrels: list, qid: int) -> None:
    """
    Converts qrels (query relevance judgments) to TREC evaluation format.

    Arguments:
    - qrels: A list of qrel lines (document IDs).
    - qid: The Query ID for this set of documents.
    """
    for line in qrels:
        doc_id = line.strip()
        if doc_id:  # skip empty lines
            print(f"{qid} 0 {doc_id} 1")


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
        # Sort the files to ensure QIDs are in order
        for qrel_file in sorted(qrels_path.glob('*.txt')):
            if qrel_file.is_file():
                
                qid_str = qrel_file.stem 
                try:
                    qid_int = int(qid_str)
                except ValueError:
                    print(f"Skipping file with non-numeric name: {qrel_file.name}", file=sys.stderr)
                    continue

                with open(qrel_file, 'r') as f:
                    qrels_to_trec(f.readlines(), qid_int)
                    
    # If it's a file, read it directly
    elif qrels_path.is_file():
        qid_str = qrels_path.stem
        try:
            qid_int = int(qid_str)
        except ValueError:
             print(f"Error: File name {qrels_path.name} is not a valid QID.", file=sys.stderr)
             sys.exit(1)
        
        with open(qrels_path, 'r') as f:
            qrels_to_trec(f.readlines(), qid_int)
    else:
        print(f"Error: Path {args.qrels} is not a valid file or directory.", file=sys.stderr)
        sys.exit(1)