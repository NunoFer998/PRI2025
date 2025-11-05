.PHONY: all evaluation clean

PYTHON := python3

EVALUATION := evaluation/evaluation.py
#METRICS := evaluation/metrics.py

# --- ADD metrics
all: evaluation 
	@echo "All scripts finished."

evaluation:
	@echo "Starting evaluation..."
	@$(PYTHON) $(EVALUATION)
	@echo "Evaluation finished!"
	@echo

#metrics:
#	@echo "Starting metrics calculation..."
#	@$(PYTHON) $(METRICS)
#	@echo "Metrics calculation finished!"
#	@echo

clean:
	@echo "Cleaning up old files..."
	@rm -rf queries
	@echo "Clean complete!"
