## Set Up Virtual Environment

If it's your first time using the virtual environment run:

```bash
python -m venv venv
source venv/bin/activate      # or .\venv\Scripts\activate on Windows
pip install -r requirements.txt
```

If not, just run:

```bash
source venv/bin/activate      # or .\venv\Scripts\activate on Windows
```

## Makefile Targets

Make sure you are in the `milestone1` directory.

1. Run all scripts inside the `src` folder:
```bash
make all   
```

2. Prepare and clean the dataset:
```bash
make prepare_and_clean  
```

3. Convert the `csv` dataset into `json` format:
```bash
make json
```

4. Make a general analysis of the final dataset:
```bash
make analysis
```

5. Make an analysis for each one of the original datasets:
```bash
make characterization
```