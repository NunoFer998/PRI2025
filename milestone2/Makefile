# Makefile
SHELL := /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "style      : runs style formatting."
	@echo "down       : stops all running services, removes containers and volumes."
	@echo "up         : start Docker daemon and Solr."
	@echo "schema     : update schema using docker/data/schema.json."
	@echo "populate   : populate Solr using docker/data/data.json."
	@echo "trec_eval  : download trec_eval source code and compile it."
	@echo "test       : run unit tests."

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
