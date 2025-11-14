#!/usr/bin/env bash
set -euxo pipefail

# run_evaluation.sh - runs the end-to-end evaluation pipeline
# NOTE: this script must be executed from the evaluation directory

# Default paths
QUERIES_DIR="queries/basic"
ENHANCED_QUERIES_DIR="queries/enhanced"
QRELS_DIR="my_qrels"
COLLECTION="diseases"
TREC_EVAL="trec_eval"  # Adjust this path if trec_eval is elsewhere

echo "=== Starting evaluation pipeline ==="
echo "Queries dir: ${QUERIES_DIR}"
echo "Qrels dir: ${QRELS_DIR}"
echo "Collection: ${COLLECTION}"
echo ""

# Convert qrels to TREC format
echo "Step 1: Converting qrels to TREC format..."
./scripts/qrels2trec.py --qrels "${QRELS_DIR}" > qrels_trec.txt
echo "✓ Created qrels_trec.txt"
echo ""

# Query Solr and convert results to TREC format
echo "Step 2: Querying Solr and converting to TREC format..."
> results_trec.txt  # Clear/create empty file

for query_file in "${QUERIES_DIR}"/*; do
    if [ -f "$query_file" ]; then
        # Extract QID from filename, e.g., "0001.json" -> "0001"
        QID=$(basename "$query_file" .json)
        echo "  Processing ${QID}..."
        
        ./scripts/query_solr.py \
            --query "$query_file" \
            --collection "${COLLECTION}" \
        | ./scripts/solr2trec.py --qid "$QID" >> results_trec.txt
    fi
done

for query_file in "${ENHANCED_QUERIES_DIR}"/*; do
    if [ -f "$query_file" ]; then
        # Extract QID from filename, e.g., "0001.json" -> "0001"
        QID=$(basename "$query_file" .json)
        echo "  Processing ${QID} (enhanced)..."

        ./scripts/query_solr.py \
            --query "$query_file" \
            --collection "${COLLECTION}" \
        | ./scripts/solr2trec.py --qid "$QID" >> results_trec.txt
    fi
done
echo "✓ Created results_trec.txt"
echo ""

# Run evaluation pipeline and plot
echo "Step 3: Running trec_eval and generating plots..."
# Make sure trec_eval path is correct
./${TREC_EVAL}/trec_eval \
    -q -m all_trec \
    qrels_trec.txt results_trec.txt \
| tee eval_results.txt
# Note: Piping to two commands can be tricky with error handling.
# Let's check if eval_results.txt was created and is not empty before plotting
if [ -s eval_results.txt ]; then
    ./scripts/plot_pr.py --eval_file eval_results.txt --output pr_curve.png
    echo "✓ Evaluation complete"
else
    echo "✗ Evaluation failed. eval_results.txt is empty."
fi
echo ""

# Cleanup
# rm -f qrels_trec.txt results_trec.txt