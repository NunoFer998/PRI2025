#!/bin/bash
# Quick start script for milestone3
# Assumes Solr container already exists with data loaded

CONTAINER_NAME="meic_solr"
HOST_PORT=8983

echo "Starting Solr container..."

# Check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container '${CONTAINER_NAME}' does not exist."
    echo "Please run the full startup.sh from milestone2 first to create it."
    exit 1
fi

# Start container if not running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    docker start ${CONTAINER_NAME}
    echo "Container started."
else
    echo "Container is already running."
fi

# Wait for Solr to be ready
echo "Waiting for Solr to be ready..."
until curl -s "http://localhost:${HOST_PORT}/solr/admin/cores" > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
echo "Solr is ready at http://localhost:${HOST_PORT}"

# Verify diseases core is accessible  
if curl -s -o /dev/null -w "%{http_code}" http://localhost:${HOST_PORT}/solr/diseases/select | grep -q "200"; then
    echo "Diseases core is accessible."
else
    echo "Warning: Diseases core may not be ready."
fi

echo "Done! You can now run: make run"
