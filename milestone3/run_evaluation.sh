#!/usr/bin/env bash
# Evaluation script for Milestone 3
# Evaluates: enhanced, treatmentSort, semantic, and hybrid search systems
set -euxo pipefail

# --- CONFIGURATION ---
QUERIES_DIR="queries"
SYSTEMS_DIR="queries/systems"
QRELS_DIR="queries/my_qrels"
COLLECTION="diseases"
TREC_EVAL_BIN="trec_eval/trec_eval"
SOLR_URL="http://localhost:8983/solr"

# Python from venv
PYTHON=".venv/bin/python"

# Output files
QRELS_FILE="qrels_trec.txt"

# Standard systems (use query_solr.py)
STANDARD_SYSTEMS=(
    "enhanced" "${SYSTEMS_DIR}/enhanced.json" "results_enhanced.txt" "eval_results_enhanced.txt"
    "treatment_sorting" "${SYSTEMS_DIR}/treatmentSort.json" "results_treatment.txt" "eval_results_treatment_sorting.txt"
)

# Semantic systems (require embeddings)
SEMANTIC_SYSTEMS=(
    "semantic" "results_semantic.txt" "eval_results_semantic.txt"
    "hybrid" "results_hybrid.txt" "eval_results_hybrid.txt"
)

# --- FUNCTION: Run queries for standard systems ---
run_standard_queries() {
    local run_id="$1"
    local system_path="$2"
    local results_file="$3"
    
    echo "Processing standard run: ${run_id} (using ${system_path})"
    > "$results_file"
    
    for query_file in "${QUERIES_DIR}"/*.json; do
        if [ -f "$query_file" ]; then
            QID=$(basename "$query_file" .json)
            echo "  Processing ${QID} (${run_id})..."
            
            ${PYTHON} ./scripts/query_solr.py \
                --query "$query_file" \
                --system "$system_path" \
                --collection "${COLLECTION}" \
            | ${PYTHON} ./scripts/solr2trec.py --qid "$QID" --run-id "$run_id" >> "$results_file"
        fi
    done
}

# --- FUNCTION: Run semantic search queries ---
run_semantic_queries() {
    local run_id="$1"
    local results_file="$2"
    
    echo "Processing semantic run: ${run_id}"
    > "$results_file"
    
    for query_file in "${QUERIES_DIR}"/*.json; do
        if [ -f "$query_file" ]; then
            QID=$(basename "$query_file" .json)
            QUERY_TEXT=$(${PYTHON} -c "import json; print(json.load(open('$query_file'))['query'])")
            echo "  Processing ${QID} (${run_id}): ${QUERY_TEXT}"
            
            # Generate embedding and query using semantic.json config
            ${PYTHON} -c "
import sys
sys.path.insert(0, '.')
import json
import requests
from scripts.query_embedding import text_to_embedding

query_text = '''${QUERY_TEXT}'''
embedding = text_to_embedding(query_text)
top_k = 20

# Load semantic config
with open('${SYSTEMS_DIR}/semantic.json') as f:
    semantic_config = json.load(f)

params = {
    'q': semantic_config.get('q', '{!knn f=vector topK=\$TOP_K}\$VECTOR').replace('\$VECTOR', embedding).replace('\$TOP_K', str(top_k)),
    'fl': semantic_config.get('fl', 'id,name,symptoms,description,treatments,score'),
    'rows': 75,
    'wt': 'json'
}

response = requests.post('${SOLR_URL}/${COLLECTION}/select', data=params)
print(json.dumps(response.json()))
" | ${PYTHON} ./scripts/solr2trec.py --qid "$QID" --run-id "$run_id" >> "$results_file"
        fi
    done
}

# --- FUNCTION: Run hybrid search queries ---
run_hybrid_queries() {
    local run_id="$1"
    local results_file="$2"
    
    echo "Processing hybrid run: ${run_id}"
    > "$results_file"
    
    for query_file in "${QUERIES_DIR}"/*.json; do
        if [ -f "$query_file" ]; then
            QID=$(basename "$query_file" .json)
            QUERY_TEXT=$(${PYTHON} -c "import json; print(json.load(open('$query_file'))['query'])")
            echo "  Processing ${QID} (${run_id}): ${QUERY_TEXT}"
            
            # Generate embedding and execute hybrid query
            ${PYTHON} -c "
import sys
sys.path.insert(0, '.')
import json
import requests
from scripts.query_embedding import text_to_embedding

query_text = '''${QUERY_TEXT}'''
embedding = text_to_embedding(query_text)
top_k = 20

# Load hybrid config
with open('${SYSTEMS_DIR}/hybrid.json') as f:
    hybrid_config = json.load(f)

params = {
    'q': hybrid_config.get('q', '{!bool filter=\$Retrieval must=\$Ranking}'),
    'Retrieval': hybrid_config.get('Retrieval', '{!bool should=\$LexicalQ should=\$SemanticQ}'),
    'Ranking': hybrid_config.get('Ranking', '{!func}product(query(\$normLexical), query(\$normSemantic))'),
    'normLexical': hybrid_config.get('normLexical', '{!func}scale(query(\$LexicalQ), 0, 1)'),
    'normSemantic': hybrid_config.get('normSemantic', '{!func}scale(query(\$SemanticQ), 0, 1)'),
    'LexicalQ': hybrid_config.get('LexicalQ', \"{!edismax qf='name^10 symptoms^5 treatments^20 description^30' q.op=OR}\$QUERY\").replace('\$QUERY', query_text),
    'SemanticQ': hybrid_config.get('SemanticQ', '{!knn f=vector topK=\$TOP_K}\$VECTOR').replace('\$VECTOR', embedding).replace('\$TOP_K', str(top_k)),
    'fl': 'id,name,symptoms,description,treatments,score',
    'rows': 75,
    'wt': 'json'
}

response = requests.post('${SOLR_URL}/${COLLECTION}/select', data=params)
print(json.dumps(response.json()))
" | ${PYTHON} ./scripts/solr2trec.py --qid "$QID" --run-id "$run_id" >> "$results_file"
        fi
    done
}

# --- STEP 1: Convert qrels ---
echo "Step 1: Converting qrels to TREC format..."
${PYTHON} ./scripts/qrels2trec.py --qrels "${QRELS_DIR}" > "${QRELS_FILE}"
echo "Created ${QRELS_FILE}"
echo ""

# --- STEP 2: Run queries for standard systems ---
echo "Step 2: Running queries for standard systems..."
for ((i=0; i<${#STANDARD_SYSTEMS[@]}; i+=4)); do
    RUN_ID="${STANDARD_SYSTEMS[i]}"
    SYSTEM_PATH="${STANDARD_SYSTEMS[i+1]}"
    RESULTS_FILE="${STANDARD_SYSTEMS[i+2]}"
    
    run_standard_queries "$RUN_ID" "$SYSTEM_PATH" "$RESULTS_FILE"
done
echo ""

# --- STEP 3: Run queries for semantic systems ---
echo "Step 3: Running queries for semantic systems..."
run_semantic_queries "semantic" "results_semantic.txt"
run_hybrid_queries "hybrid" "results_hybrid.txt"
echo ""

# --- STEP 4: Evaluate all systems ---
echo "Step 4: Running trec_eval for all systems..."

# Evaluate standard systems
for ((i=0; i<${#STANDARD_SYSTEMS[@]}; i+=4)); do
    RUN_ID="${STANDARD_SYSTEMS[i]}"
    RESULTS_FILE="${STANDARD_SYSTEMS[i+2]}"
    EVAL_FILE="${STANDARD_SYSTEMS[i+3]}"
    
    echo "  Evaluating ${RUN_ID}..."
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" > "${EVAL_FILE}"
        echo "  ✓ ${RUN_ID} evaluation complete: ${EVAL_FILE}"
    else
        echo "  ✗ ${RUN_ID} FAILED: ${RESULTS_FILE} is empty" 1>&2
    fi
done

# Evaluate semantic systems
for ((i=0; i<${#SEMANTIC_SYSTEMS[@]}; i+=3)); do
    RUN_ID="${SEMANTIC_SYSTEMS[i]}"
    RESULTS_FILE="${SEMANTIC_SYSTEMS[i+1]}"
    EVAL_FILE="${SEMANTIC_SYSTEMS[i+2]}"
    
    echo "  Evaluating ${RUN_ID}..."
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" > "${EVAL_FILE}"
        echo "  ✓ ${RUN_ID} evaluation complete: ${EVAL_FILE}"
    else
        echo "  ✗ ${RUN_ID} FAILED: ${RESULTS_FILE} is empty" 1>&2
    fi
done
echo ""

# --- STEP 5: Generate PR curves ---
echo "Step 5: Generating PR curve plots..."
mkdir -p evaluation_plots
rm -f evaluation_plots/*

MAX_QUERY_ID=3

# Collect all system info for plotting
ALL_SYSTEMS=(
    "enhanced" "results_enhanced.txt" "eval_results_enhanced.txt"
    "treatment_sorting" "results_treatment.txt" "eval_results_treatment_sorting.txt"
    "semantic" "results_semantic.txt" "eval_results_semantic.txt"
    "hybrid" "results_hybrid.txt" "eval_results_hybrid.txt"
)

# Generate individual query curves
echo "Generating individual query curves..."
for ((i=0; i<${#ALL_SYSTEMS[@]}; i+=3)); do
    RUN_ID="${ALL_SYSTEMS[i]}"
    RESULTS_FILE="${ALL_SYSTEMS[i+1]}"
    
    for query_id in $(seq 1 $MAX_QUERY_ID); do
        QID_PADDED=$(printf "%04d" "$query_id")
        QID_UNPADDED=$(printf "%d" "$query_id")
        
        if [ -s "${RESULTS_FILE}" ]; then
            "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
                awk -v q1="$QID_PADDED" -v q2="$QID_UNPADDED" '($2 == q1) || ($2 == q2) {print}' | \
                ${PYTHON} ./scripts/plot_pr.py --output "evaluation_plots/query_${QID_PADDED}_${RUN_ID}_pr.png"
        fi
    done
done

# Generate general system curves
echo "Generating general system curves..."
for ((i=0; i<${#ALL_SYSTEMS[@]}; i+=3)); do
    RUN_ID="${ALL_SYSTEMS[i]}"
    RESULTS_FILE="${ALL_SYSTEMS[i+1]}"
    
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
            ${PYTHON} ./scripts/plot_pr.py --output "evaluation_plots/${RUN_ID}_general_pr.png"
        echo "  ✓ General curve for ${RUN_ID}"
    fi
done

# Generate comparative plot
echo "Generating comparative plot..."
COMPARATIVE_FILE="temp_comparative_pr_data.txt"
> "${COMPARATIVE_FILE}"

for ((i=0; i<${#ALL_SYSTEMS[@]}; i+=3)); do
    RUN_ID="${ALL_SYSTEMS[i]}"
    RESULTS_FILE="${ALL_SYSTEMS[i+1]}"
    
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
            awk -v run_id="${RUN_ID}" '{print run_id, $0}' >> "${COMPARATIVE_FILE}"
    fi
done

if [ -s "${COMPARATIVE_FILE}" ]; then
    cat "${COMPARATIVE_FILE}" | ${PYTHON} ./scripts/plot_pr.py --output "evaluation_plots/comparative_all_systems_pr.png"
    echo "  ✓ Comparative plot generated"
fi

rm -f "${COMPARATIVE_FILE}"

echo ""
echo "=========================================="
echo "✓ Evaluation complete!"
echo "=========================================="
echo "Results files:"
echo "  - eval_results_enhanced.txt"
echo "  - eval_results_treatment_sorting.txt"
echo "  - eval_results_semantic.txt"
echo "  - eval_results_hybrid.txt"
echo ""
echo "Plots in: evaluation_plots/"
echo "=========================================="
