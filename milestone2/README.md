# Milestone 2

How to set up the Solr container and index the disease dataset included in this project.  

---

## Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop) installed and running  
- **For Windows users:** *WSL (Windows Subsystem for Linux)* or *Git Bash* installed  

---

### Step 1: Navigate to the Project Directory

Make sure your terminal is in the `milestone2` directory:

```bash
cd path/to/PRI2025/milestone2
```

### Step 2: Start thr Solr Container

If you already created the container and want to use it, just restart it:
```bash
docker start meic_solr # or the name of your container
```


Otherwise, this command creates a ``Solr`` container named ``meic_solr``, maps the local data folder to the container, and pre-creates the ``diseases`` collection.

For Linux / WSL / Mac:
```bash
docker run -p 8983:8983 --name meic_solr -v "${PWD}/data:/data" -d solr:9 solr-precreate diseases
```

For Windows Command Prompt / Powershell:
```bash
docker run -p 8983:8983 --name meic_solr -v ${PWD}/data:/data -d solr:9 solr-precreate diseases
```

### Step 3: Run the Startup Script

The script will:

1. Apply the schema (``schema.json``) to the ``diseases`` collection
2. Post the CSV dataset (``merged_disease_symptom_list.csv``) to ``Solr``
3. Commit the changes so the data becomes searchable

Make the script executable (Linux / WSL / Mac):
```bash
chmod +x startup.sh
```

Run it:
```bash
./startup.sh
```

### Step 4: Access Solr

Use the Solr Admin UI to explore the ``diseases`` collection.

Open a browser and go to:

```bash
http://localhost:8983
```

