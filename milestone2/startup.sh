#!/bin/bash
set -e

echo "Starting Solr setup..."

CONTAINER_NAME="meic_solr"
IMAGE="solr:9"
HOST_PORT=8983
LOCAL_DATA_PATH="${PWD}/data"
SOLR_CORE="diseases"

echo "Checking container status..."

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "Container '${CONTAINER_NAME}' does not exist. Creating it..."

    docker run -p ${HOST_PORT}:8983 \
        --name ${CONTAINER_NAME} \
        -v "${LOCAL_DATA_PATH}:/data" \
        -d ${IMAGE} solr-precreate ${SOLR_CORE}

else
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "Container '${CONTAINER_NAME}' exists but is stopped. Starting it...\n"
        docker start ${CONTAINER_NAME}
    else
        echo -e "Container '${CONTAINER_NAME}' is already running.\n"
    fi
fi

echo -e "\nWaiting for Solr to start...\n"
sleep 10

# Schema definition via API
echo "Checking if schema was already applied..."

FIELD_EXISTS=$(curl -s "http://localhost:8983/solr/diseases/schema/field/name" | grep -c '"name"')

if [ "$FIELD_EXISTS" -gt 0 ]; then
    echo -e "Schema already applied. Skipping schema update.\n"
else
    echo "Updating schema..."
    curl -X POST -H 'Content-type:application/json' \
        --data-binary "@data/schema.json" \
        http://localhost:8983/solr/diseases/schema

    echo -e "\nAllowing schema to reload...\n"
    sleep 5
fi

# Populate collection using mapped path inside container.
echo -e "Deleting old data and re-indexing...\n"
docker exec -it ${CONTAINER_NAME} bin/solr post -c ${SOLR_CORE} /data/merged_disease_symptom_list.csv -params "overwrite=true"

echo -e "\nSetup complete!"
