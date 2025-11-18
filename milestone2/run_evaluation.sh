#!/usr/bin/env bash
# Define strict execution rules: exit on error, use unset variables, fail pipes, enable debugging
set -euxo pipefail

# --- CONFIGURAÇÕES E VARIÁVEIS ---
QUERIES_DIR="queries"
SYSTEMS_DIR="queries/systems"
QRELS_DIR="my_qrels"
COLLECTION="diseases"
TREC_EVAL_BIN="trec_eval/trec_eval" 

# Ficheiros e Variáveis
QRELS_FILE="qrels_trec.txt"
EVAL_BASIC="eval_results_basic.txt"
EVAL_ENHANCED="eval_results_enhanced.txt"

# Array de sistemas a serem avaliados
# O formato é: (RUN_ID SISTEMA_FILE RESULTADOS_FILE EVAL_FILE)
# Se precisar de um sistema de 'treatment_sorting', adicione-o aqui
SYSTEMS=(
    "basic" "${SYSTEMS_DIR}/basic.json" "results_basic.txt" "${EVAL_BASIC}"
    "enhanced" "${SYSTEMS_DIR}/enhanced.json" "results_enhanced.txt" "${EVAL_ENHANCED}"
    "treatment_sorting" "${SYSTEMS_DIR}/treatmentSort.json" "results_treatment.txt" "eval_results_treatment_sorting.txt"
)
# Se o seu sistema de 'treatment_sorting' estiver pronto, adicione-o assim:
# SYSTEMS=(
#     ...
#     "treatment_sorting" "${SYSTEMS_DIR}/treatment_sorting.json" "results_treatment.txt" "eval_results_treatment_sorting.txt"
# )

echo "Starting evaluation pipeline..."
echo "Collection: ${COLLECTION}"
echo ""

# --- FUNÇÃO DE EXECUÇÃO DE CONSULTA ---
# Argumentos: 1=RUN_ID, 2=SYSTEM_PATH, 3=RESULTS_FILE
run_queries() {
    local run_id="$1"
    local system_path="$2"
    local results_file="$3"
    
    echo "Processing run: ${run_id} (using ${system_path}). Outputting to ${results_file}"
    
    # Limpa o ficheiro de resultados anterior para esta execução
    > "$results_file"
    
    # Itera sobre todos os ficheiros .json no diretório de consultas
    for query_file in "${QUERIES_DIR}"/*.json; do
        if [ -f "$query_file" ]; then
            QID=$(basename "$query_file" .json)
            echo "  Processing ${QID} (${run_id})..."
            
            # Executa a pipeline query_solr.py | solr2trec.py
            ./scripts/query_solr.py \
                --query "$query_file" \
                --system "$system_path" \
                --collection "${COLLECTION}" \
            | ./scripts/solr2trec.py --qid "$QID" --run-id "$run_id" >> "$results_file"
        fi
    done
}
# -----------------------------------


# --- ETAPA 1: CONVERTER QRELS ---
echo "Step 1: Converting qrels to TREC format..."
./scripts/qrels2trec.py --qrels "${QRELS_DIR}" > "${QRELS_FILE}"
echo "Created ${QRELS_FILE}"
echo ""


# --- ETAPA 2: EXECUTAR CONSULTAS PARA TODOS OS SISTEMAS ---
echo "Step 2: Running queries for all systems..."

# Itera sobre o array SYSTEMS
# O @ na expansão garante que os elementos são tratados em grupos de 4 (RUN_ID, PATH, FILE, EVAL_FILE)
for ((i=0; i<${#SYSTEMS[@]}; i+=4)); do
    RUN_ID="${SYSTEMS[i]}"
    SYSTEM_PATH="${SYSTEMS[i+1]}"
    RESULTS_FILE="${SYSTEMS[i+2]}"
    
    run_queries "$RUN_ID" "$SYSTEM_PATH" "$RESULTS_FILE"
done

echo ""


# --- ETAPA 3: AVALIAR TODOS OS SISTEMAS ---
echo "Step 3: Running trec_eval for all systems..."

for ((i=0; i<${#SYSTEMS[@]}; i+=4)); do
    RUN_ID="${SYSTEMS[i]}"
    RESULTS_FILE="${SYSTEMS[i+2]}"
    EVAL_FILE="${SYSTEMS[i+3]}"
    
    echo "  Evaluating ${RUN_ID} run..."
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" > "${EVAL_FILE}"
        echo "${RUN_ID} evaluation complete. Results in ${EVAL_FILE}"
    else
        echo "Evaluation FAILED: ${RESULTS_FILE} is empty. Check Solr connection or query syntax." 1>&2
    fi
done

echo ""


# --- ETAPA 4: GERAR CURVAS PR ---
echo "Step 4: Generating PR curve plots..."
mkdir -p evaluation_plots
# Limpa a pasta
rm -f evaluation_plots/*

# --- Geração de Gráficos Individuais e Médios ---

# Determina o número máximo de query_id que você tem (ex: se tem 10, use {1..10})
# Assumindo que os QIDs são numerados de 0001 até N:
# NOTA: O seu código original itera de {1..3}. Vou manter este intervalo como exemplo.
MAX_QUERY_ID=3
echo "Generating individual query curves (1-${MAX_QUERY_ID})..."

for ((i=0; i<${#SYSTEMS[@]}; i+=4)); do
    RUN_ID="${SYSTEMS[i]}"
    RESULTS_FILE="${SYSTEMS[i+2]}"
    
    # 1. Geração de Curvas Individuais
    for query_id in $(seq 1 $MAX_QUERY_ID); do
        # O QID é preenchido com zeros, ex: 0001
        QID_PADDED=$(printf "%04d" "$query_id")

        if [ -s "${RESULTS_FILE}" ]; then
            "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
                # Filtra apenas a query atual (QID)
                awk -v qid="$QID_PADDED" '$2 == qid {print}' | \
                ./scripts/plot_pr.py --output "evaluation_plots/query_${QID_PADDED}_${RUN_ID}_pr.png" 2>/dev/null || true
        fi
    done
    
    # 2. Geração da Curva Média (OVERALL average)
    echo "  Generating average curve for ${RUN_ID}..."
    if [ -s "${RESULTS_FILE}" ]; then
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
            # Filtra a linha de resultados "all"
            awk '$2 == "all" {print}' | \
            ./scripts/plot_pr.py --output "evaluation_plots/average_${RUN_ID}_pr.png" 2>/dev/null || true
    fi
done

echo "✓ Done. Reports available in evaluation_plots/"