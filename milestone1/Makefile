PYTHON := python

.DEFAULT_GOAL := all

all: 
	@echo Running all scripts.
	@echo All scripts finished successfully.

prepare_and_clean:
	@echo Preparing and cleaning data...
	$(PYTHON) dags/data_preparation_and_cleaning.py
	@echo Preparation and cleaning done.

json:
	@echo Converting the CSV to JSON.
	$(PYTHON) dags/convert_to_json.py
	@echo Conversion finished.

analysis:
	@echo Running data analysis...
	$(PYTHON) dags/summary_analysis.py
	@echo Analysis done.

characterization:
	@echo Analysing all the datasets...
	$(PYTHON) dags/data_characterization.py
	@echo "Characterization done."