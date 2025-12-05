# Milestone 3

## Prerequisites
- WSL / Linux
- Python installed
- Docker Desktop (running and connected to WSL for the Solr database)

## Commands

### 1. Install Dependencies

Creates a Python virtual environment (.venv) and installs everything listed in ``requirements.txt``.

````bash
make install
````

### 2. Start the Solr Database

Navigates to the ``milestone2`` folder and launches the Solr container script (``startup.sh``).

````bash
make solr
````

### 3. Run the Interface

Starts the Flask application (``interface/interface.py``).

````bash
make run
````

### 4. Clean Up

Removes the virtual environment (``.venv``) and deletes all cached python files (``__pycache__, .pyc``). 

Use this if you want to perform a fresh installation.

````bash
make clean
````