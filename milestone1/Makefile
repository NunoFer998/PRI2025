PYTHON := python

.DEFAULT_GOAL := all

all: 
	@echo Running all scripts...
	@echo All scripts finished successfully.

prepare_and_clean:
	@echo Preparing and cleaning data...
	$(PYTHON) src/data_preparation_and_cleaning.py
	@echo Preparation and cleaning done.

json:
	@echo Converting the CSV to JSON.
	$(PYTHON) src/convert_to_json.py
	@echo Conversion finished.

analysis:
	@echo Running data analysis...
	$(PYTHON) src/summary_analysis.py
	@echo Analysis done.

characterization:
	@echo Analysing all the datasets...
	$(PYTHON) src/data_characterization.py
	@echo Characterization done.