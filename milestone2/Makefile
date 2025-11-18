SHELL := /bin/bash

# Default target
.DEFAULT_GOAL := all

.PHONY: help
help:
	@echo "Commands:"
	@echo "style        : runs style formatting."
	@echo "down         : stops all running services, removes containers and volumes."
	@echo "up           : start Docker daemon and Solr."
	@echo "schema       : update schema using docker/data/schema.json."
	@echo "populate     : populate Solr using docker/data/data.json."
	@echo "trec_eval    : download trec_eval source code and compile it."
	@echo "test         : run unit tests."
	@echo "startup      : run startup.sh."
	@echo "evaluation   : run run_evaluation.sh."
	@echo "all          : run startup then evaluation."

.PHONY: convert_unix
convert_unix:
	@if command -v dos2unix >/dev/null 2>&1; then \
		echo "Converting scripts to Unix format..."; \
		dos2unix startup.sh >/dev/null 2>&1 || true; \
		dos2unix run_evaluation.sh >/dev/null 2>&1 || true; \
	else \
		echo "WARNING: dos2unix not installed. Scripts may fail if they have Windows CRLF endings."; \
	fi

.PHONY: style
style:
	isort src test --atomic
	black -l 100 src test
	flake8 src test

.PHONY: down
down:
	docker compose -f docker/docker-compose.yml down --remove-orphans -v

.PHONY: up
up:
	docker compose -f docker/docker-compose.yml up -d

.PHONY: schema
schema:
	curl -X POST \
		-H 'Content-type:application/json' \
		--data-binary "@./docker/data/simple_schema.json" \
		http://localhost:8983/solr/courses/schema

.PHONY: populate
populate:
	docker exec -it solr_pri bin/solr post -c courses /data/meic_courses.json

.PHONY: trec_eval
trec_eval:
	git clone https://github.com/usnistgov/trec_eval.git trec_eval
	cd trec_eval && make
	cd ..

.PHONY: test
test:
	python -m unittest discover -s test -p 'test_*.py'


.PHONY: startup
startup: convert_unix
	@if [ ! -x startup.sh ]; then chmod +x startup.sh; fi
	./startup.sh

.PHONY: evaluation
evaluation: convert_unix
	@if [ ! -x run_evaluation.sh ]; then chmod +x run_evaluation.sh; fi
	./run_evaluation.sh

.PHONY: all
all: startup evaluation
