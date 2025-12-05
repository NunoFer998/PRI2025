# Variables
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

# SOLR Variables 
SOLR_DIR := ../milestone2
SOLR_SCRIPT := ./startup.sh  

.PHONY: all
all: install

.PHONY: solr
solr:
	@echo "Starting Solr..."
	cd $(SOLR_DIR) && $(SOLR_SCRIPT)


$(VENV)/bin/activate:
	python3 -m venv $(VENV)

.PHONY: install
install: $(VENV)/bin/activate requirements.txt
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

.PHONY: run
run: install
	$(PYTHON) interface/interface.py

.PHONY: clean
clean:
	rm -rf $(VENV)
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete