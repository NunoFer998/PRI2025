# Milestone 2

How to set up the Solr container and index the disease dataset included in this project.  

---

## Prerequisites

For indexing the data and creating the collections:
- [Docker](https://www.docker.com/products/docker-desktop) installed and running  
- **For Windows users:** *WSL (Windows Subsystem for Linux)* or *Git Bash* installed  

For running the scripts and calculating the metrics:
- ```python3``` installed
- A **Solr instance** running at the localhost

---

### Step 1: Navigate to the Project Directory

Make sure your terminal is in the `milestone2` directory:

```bash
cd path/to/PRI2025/milestone2
```

### Step 2: Run the Startup Script

The provided script will:

1. Detect whether the container ``meic_solr`` already exists
2. Start it if it exists, but is stopped
3. Create it automatically (with the correct configuration) if it does not exist.
4. Apply the schema (``schema.json``) to the ``diseases`` collection
5. Post the CSV dataset (``merged_disease_symptom_list.csv``) to ``Solr``
5. Commit the changes so the data becomes searchable

Run it:
```bash
make startup
```

### Step 3: Access Solr

Use the Solr Admin UI to explore the ``diseases`` collection.

Open a browser and go to:

```bash
http://localhost:8983
```

### Step 4: Running the Scripts

To run the evaluation scripts:

```bash
make evaluation
```