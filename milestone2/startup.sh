#!/bin/bash

# This script expects a container started with the following command.
# Windows:
# docker run -p 8983:8983 --name meic_solr -v ${PWD}:/data -d solr:9 solr-precreate diseases
# Linux:
# docker run -p 8983:8983 --name meic_solr -v "${PWD}/data:/data" -d solr:9 solr-precreate diseases

sleep 10

# Schema definition via API
echo "Updating schema..."

curl -X POST -H 'Content-type:application/json' \
    --data-binary "@data/schema.json" \
    http://localhost:8983/solr/diseases/schema

echo "Allowing schema to reload..."
sleep 5

# Populate collection using mapped path inside container.
echo "Deleting old data and re-indexing..."
docker exec -it meic_solr bin/solr post -c diseases /data/merged_disease_symptom_list.csv -params "overwrite=true"

echo "Setup complete"