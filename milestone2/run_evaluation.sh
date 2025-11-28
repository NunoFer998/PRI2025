#!/usr/bin/env bash
# Define strict execution rules: exit on error, use unset variables, fail pipes, enable debugging
set -euxo pipefail

# --- CONFIGURAÇÕES E VARIÁVEIS ---
QUERIES_DIR="queries"
SYSTEMS_DIR="queries/systems"
QRELS_DIR="queries/my_qrels"
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
         # Also compute unpadded numeric QID (matches qrels2trec.py output)
        QID_UNPADDED=$(printf "%d" "$query_id")

        if [ -s "${RESULTS_FILE}" ]; then
            "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
                awk -v q1="$QID_PADDED" -v q2="$QID_UNPADDED" '($2 == q1) || ($2 == q2) {print}' | \
                ./scripts/plot_pr.py --output "evaluation_plots/query_${QID_PADDED}_${RUN_ID}_pr.png"
        fi
    done
    
done


echo ""

# --- ETAPA 4B: GERAR CURVAS GERAIS PARA CADA SISTEMA ---
echo "Generating general system curves..."

for ((i=0; i<${#SYSTEMS[@]}; i+=4)); do
    RUN_ID="${SYSTEMS[i]}"
    RESULTS_FILE="${SYSTEMS[i+2]}"
    EVAL_FILE="${SYSTEMS[i+3]}"
    
    echo "  Generating general curve for ${RUN_ID} system..."
    if [ -s "${EVAL_FILE}" ]; then
        # Gera curva geral usando o ficheiro de avaliação completo
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
            ./scripts/plot_pr.py --output "evaluation_plots/${RUN_ID}_general_pr.png"
        echo "  ✓ General curve for ${RUN_ID}: evaluation_plots/${RUN_ID}_general_pr.png"
    fi
done

# --- ETAPA 4C: GERAR CURVAS COMPARATIVAS DOS SISTEMAS ---
echo "Generating comparative system curves..."

# 1. Crie um ficheiro temporário para armazenar todas as curvas
COMPARATIVE_RESULTS_FILE="temp_comparative_pr_data.txt"
> "${COMPARATIVE_RESULTS_FILE}" # Limpa o ficheiro se já existir

# 2. Percorra todos os sistemas e anexe os dados ao ficheiro temporário
for ((i=0; i<${#SYSTEMS[@]}; i+=4)); do
    RUN_ID="${SYSTEMS[i]}"
    RESULTS_FILE="${SYSTEMS[i+2]}"
    EVAL_FILE="${SYSTEMS[i+3]}"
    
    echo "  Collecting data for ${RUN_ID} system..."

    if [ -s "${EVAL_FILE}" ]; then
        # Gera a curva de Precisão-Recuperação e anexa os dados ao ficheiro temporário.
        # Usa o nome do sistema (RUN_ID) como prefixo/label.
        "${TREC_EVAL_BIN}" -q -m all_trec "${QRELS_FILE}" "${RESULTS_FILE}" | \
            awk -v run_id="${RUN_ID}" '{print run_id, $0}' >> "${COMPARATIVE_RESULTS_FILE}"
        echo "  ✓ Data collected for ${RUN_ID}."
    else
        echo "  ⚠ Skipping ${RUN_ID}: ${EVAL_FILE} is empty."
    fi
done

# 3. Gere o gráfico comparativo usando o ficheiro temporário
if [ -s "${COMPARATIVE_RESULTS_FILE}" ]; then
    echo "  Generating comparative plot..."
    
    # O script plot_pr.py deve ser capaz de ler o ficheiro multi-sistema.
    # Pode ser necessário adaptar o seu script plot_pr.py para ler este novo formato.
    cat "${COMPARATIVE_RESULTS_FILE}" | \
        ./scripts/plot_pr.py --output "evaluation_plots/comparative_all_systems_pr.png"

    echo "  ✨ Comparative plot generated: evaluation_plots/comparative_all_systems_pr.png"
else
    echo "  ❌ No data collected. Cannot generate comparative plot."
fi

# 4. Limpe o ficheiro temporário (opcional)
rm -f "${COMPARATIVE_RESULTS_FILE}"

echo "✓ Done. Reports available in evaluation_plots/"