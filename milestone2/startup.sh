#!/bin/bash
set -e

echo "Starting Solr setup..."

# Stop & remove existing container if present
docker stop meic_solr >/dev/null 2>&1 || true
docker rm meic_solr >/dev/null 2>&1 || true

# Start Solr container and precreate core
docker run -p 8983:8983 --name meic_solr \
    -v "${PWD}/data:/data" \
    -d solr:9 solr-precreate diseases

# --- STEP 1: Wait for Solr service to start (General Solr availability) ---
echo "Waiting for Solr service to start..."
until curl -s http://localhost:8983/solr/ >/dev/null; do
  sleep 2
done

# --- STEP 2: CRITICAL FIX: Wait for 'diseases' core to be fully ready before schema update ---
echo "Waiting for 'diseases' core to be fully ready..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
  echo -n "."
  sleep 2
done
echo "Core is ready."# --- STEP 2.5: CRITICAL FIX: Copy synonyms file to Solr BEFORE schema update ---
if [ -f "data/synonyms_diseases.txt" ]; then
  echo "Copying synonyms file to Solr..."
  # This command places the file in the core's conf directory
  docker cp data/synonyms_diseases.txt meic_solr:/var/solr/data/diseases/conf/synonyms_diseases.txt
  echo "Successfully copied synonyms file."
else
  # This warning indicates a file is missing. The next step (schema update) will fail.
  echo "Warning: synonyms_diseases.txt not found in data/ directory. Schema update will likely fail."
fi

# --- STEP 3: Schema Update (Must succeed now that the dependency file is present) ---
echo "Updating schema..."
curl -s -X POST -H 'Content-type:application/json' \
  --data-binary "@data/schema.json" \
  http://localhost:8983/solr/diseases/schema

# --- STEP 4: MANDATORY WAIT: Wait for core to process the schema change/reload ---
# If the schema update fails, this step might hang or only respond with errors.
echo "Waiting for core to stabilize after schema update..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
  echo -n "."
  sleep 2
done
echo "Core is stable."

# The rest of the script (deleting and re-indexing) can remain as is.

# --- STEP 4.5: Copy synonyms file to Solr ---
if [ -f "data/synonyms_diseases.txt" ]; then
    echo "Copying synonyms file to Solr..."
    docker cp data/synonyms_diseases.txt meic_solr:/var/solr/data/diseases/conf/synonyms_diseases.txt
    
    # Reload the core to pick up the synonyms file
    echo "Reloading core to apply synonyms..."
    curl -s "http://localhost:8983/solr/admin/cores?action=RELOAD&core=diseases" >/dev/null
    
    # Wait for reload to complete
    echo "Waiting for core reload..."
    until curl -s -o /dev/null -w "%{http_code}" http://localhost:8983/solr/diseases/select | grep -q "200"; do
      echo -n "."
      sleep 1
    done
    echo "Synonyms applied successfully."
else
    echo "Warning: synonyms_diseases.txt not found in data/ directory. Skipping synonyms setup."
fi

# --- STEP 5: Delete old data and Re-index (Needed after a successful schema change) ---
echo "Deleting old data and re-indexing..."

# 1. Delete all existing data (Must be done to re-index correctly)
curl -s -X POST -H 'Content-type:application/json' \
    --data-binary '{"delete": {"query":"*:*"}}' \
    http://localhost:8983/solr/diseases/update?commit=true

# 2. Re-index the data against the new schema
docker exec meic_solr bin/solr post -c diseases /data/merged_disease_symptom_list.csv -params "overwrite=true"

echo "Setup completed"