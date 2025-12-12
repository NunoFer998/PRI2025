#!/bin/bash
set -e

docker stop meic_solr 2>/dev/null || true
docker rm meic_solr 2>/dev/null || true 

echo "Starting Solr setup..."

CONTAINER_NAME="meic_solr"
IMAGE="solr:9"
HOST_PORT=8983
LOCAL_DATA_PATH="${PWD}/data"
SOLR_CORE="diseases"

echo "Checking container status..."

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container '${CONTAINER_NAME}' does not exist. Creating it..."
    docker run -p ${HOST_PORT}:8983 \
        --name ${CONTAINER_NAME} \
        -v "${LOCAL_DATA_PATH}:/data" \
        -d ${IMAGE} solr-precreate ${SOLR_CORE}
else
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container '${CONTAINER_NAME}' exists but is stopped. Starting it..."
        docker start ${CONTAINER_NAME}
    else
        echo "Container '${CONTAINER_NAME}' is already running."
    fi
fi

echo "Waiting for Solr to be ready..."
until curl -s "http://localhost:${HOST_PORT}/solr/admin/cores" > /dev/null; do
    echo "Solr not ready yet, waiting 5s..."
    sleep 5
done
echo "Solr is ready!"

echo "Waiting for 'diseases' core to be fully ready..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
  echo -n "."
  sleep 2
done
if [ -f "data/synonyms_diseases.txt" ]; then
    if docker exec meic_solr test -f /var/solr/data/diseases/conf/synonyms_diseases.txt; then
        echo "Synonyms file already exists in container, skipping copy."
    else
        echo "Copying synonyms file to Solr..."
        docker cp data/synonyms_diseases.txt meic_solr:/var/solr/data/diseases/conf/synonyms_diseases.txt
        echo "Successfully copied synonyms file."
    fi
else
    echo "Warning: synonyms_diseases.txt not found in data/ directory. Schema update will likely fail."

fi

echo "Updating schema..."
curl -s -X POST -H 'Content-type:application/json' \
  --data-binary "@data/schema.json" \
  http://localhost:8983/solr/diseases/schema

echo "Waiting for core to stabilize after schema update..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
  echo -n "."
  sleep 2
done
echo "Core is stable."

if [ -f "data/synonyms_diseases.txt" ]; then
    if docker exec meic_solr test -f /var/solr/data/diseases/conf/synonyms_diseases.txt; then
        echo "Synonyms file already exists in container, skipping copy and reload."
    else
        echo "Copying synonyms file to Solr..."
        docker cp data/synonyms_diseases.txt meic_solr:/var/solr/data/diseases/conf/synonyms_diseases.txt
        
        echo "Reloading core to apply synonyms..."
        curl -s "http://localhost:8983/solr/admin/cores?action=RELOAD&core=diseases" >/dev/null
        
        echo "Waiting for core reload..."
        until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
          echo -n "."
          sleep 1
        done
        echo "Synonyms applied successfully."
    fi

else
    echo "Warning: synonyms_diseases.txt not found in data/ directory. Skipping synonyms setup."
fi

echo "Deleting old data and re-indexing..."

curl -s -X POST -H 'Content-type:application/json' \
    --data-binary '{"delete": {"query":"*:*"}}' \
    http://localhost:8983/solr/diseases/update?commit=true

docker cp data/semantic_dataset.json meic_solr:/opt/solr-9.10.0/

docker exec -w /opt/solr-9.10.0 meic_solr \
  bin/solr post -c ${SOLR_CORE} semantic_dataset.json

echo "Setup completed"