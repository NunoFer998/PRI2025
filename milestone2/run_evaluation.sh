#!/usr/bin/env bash
set -euxo pipefail

# Default paths
QUERIES_DIR="queries/basic"
ENHANCED_QUERIES_DIR="queries/enhanced"
QRELS_DIR="my_qrels"
COLLECTION="diseases"
TREC_EVAL_BIN="trec_eval/trec_eval" 

QRELS_FILE="qrels_trec.txt"
RESULTS_BASIC="results_basic.txt"
RESULTS_ENHANCED="results_enhanced.txt"
EVAL_BASIC="eval_results_basic.txt"
EVAL_ENHANCED="eval_results_enhanced.txt"

echo "Starting evaluation pipeline..."
echo "Collection: ${COLLECTION}"
echo ""

# Convert qrels 
echo "Step 1: Converting qrels to TREC format..."
./scripts/qrels2trec.py --qrels "${QRELS_DIR}" > "${QRELS_FILE}"
echo "Created ${QRELS_FILE}"
echo ""

# Run BASIC queries 
echo "Step 2a: Running BASIC queries..."
> "${RESULTS_BASIC}"
for query_file in "${QUERIES_DIR}"/*; do
    if [ -f "$query_file" ]; then
        QID=$(basename "$query_file" .json)
        echo "  Processing ${QID} (basic)..."
        ./scripts/query_solr.py \
            --query "$query_file" \
            --collection "${COLLECTION}" \
        | ./scripts/solr2trec.py --qid "$QID" --run-id "basic" >> "${RESULTS_BASIC}"
    fi
done
echo "✓ Created ${RESULTS_BASIC}"
echo ""

# Run ENHANCED queries 
echo "Step 2b: Running ENHANCED queries..."
> "${RESULTS_ENHANCED}" 
for query_file in "${ENHANCED_QUERIES_DIR}"/*; do
    if [ -f "$query_file" ]; then
        QID=$(basename "$query_file" .json)
        echo "  Processing ${QID} (enhanced)..."
        ./scripts/query_solr.py \
            --query "$query_file" \
            --collection "${COLLECTION}" \
        | ./scripts/solr2trec.py --qid "$QID" --run-id "enhanced" >> "${RESULTS_ENHANCED}"
    fi
done
echo "Created ${RESULTS_ENHANCED}"
echo ""

# Run trec_eval (separately for each run) 
echo "Step 3: Running trec_eval..."

# Evaluate BASIC run
echo "  Evaluating BASIC run..."
if [ -s "${RESULTS_BASIC}" ]; then
    "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_BASIC}" > "${EVAL_BASIC}"
    echo "Basic evaluation complete. Results in ${EVAL_BASIC}"
else
    echo "Evaluation failed: ${RESULTS_BASIC} is empty."
fi

# Evaluate ENHANCED run
echo "  Evaluating ENHANCED run..."
if [ -s "${RESULTS_ENHANCED}" ]; then
    "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_ENHANCED}" > "${EVAL_ENHANCED}"
    echo "Enhanced evaluation complete. Results in ${EVAL_ENHANCED}"
else
    echo "Evaluation failed: ${RESULTS_ENHANCED} is empty."
fi
echo ""

echo "Step 4: Generating PR curve plots..."
mkdir -p evaluation_plots
# Empty the folder
rm -rf evaluation_plots/*

# Generate PR curves for each individual query
echo "Generating individual query curves..."
for query_id in {1..3}; do
    # BASIC - filter by query ID
    "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_BASIC}" | \
        awk -v qid="$query_id" '$2 == qid {print}' | \
        ./scripts/plot_pr.py --output "evaluation_plots/query_${query_id}_basic_pr.png" 2>/dev/null || true
    
    # ENHANCED - filter by query ID
    "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_ENHANCED}" | \
        awk -v qid="$query_id" '$2 == qid {print}' | \
        ./scripts/plot_pr.py --output "evaluation_plots/query_${query_id}_enhanced_pr.png" 2>/dev/null || true
done

echo "✓ Generated individual query curves (1-10)"

# Generate OVERALL average curves (both systems)
echo "Generating overall average curves..."
"${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_BASIC}" | \
    awk '$2 == "all" {print}' | \
    ./scripts/plot_pr.py --output "evaluation_plots/average_basic_pr.png" 2>/dev/null || true

"${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_ENHANCED}" | \
    awk '$2 == "all" {print}' | \
    ./scripts/plot_pr.py --output "evaluation_plots/average_enhanced_pr.png" 2>/dev/null || true

echo "✓ Generated average curves for both systems"

# Cleanup
# echo "Step 5: Cleaning up temporary files..."
#  -f "${QRELS_FILE}" "${RESULTS_BASIC}" "${RESULTS_ENHANCED}"
echo "✓ Done. Reports available in evaluation_plots/"