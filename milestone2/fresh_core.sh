CONTAINER_NAME="meic_solr"
IMAGE="solr:9"
HOST_PORT=8983
LOCAL_DATA_PATH="${PWD}/data"
SOLR_CORE="diseases"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    docker run -p ${HOST_PORT}:8983 \
        --name ${CONTAINER_NAME} \
        -v "${LOCAL_DATA_PATH}:/data" \
        -d ${IMAGE} solr-precreate ${SOLR_CORE}
else

# Add the schema defined at semantic_schema.json
curl -X POST -H 'Content-type:application/json' \
--data-binary "@./data/schema.json" \
http://localhost:8983/solr/diseases/schema

# Index the JSON documents.
curl -X POST -H 'Content-type:application/json' \
--data-binary "@./data/semantic_courses.json" \
http://localhost:8983/solr/diseases/update?commit=true