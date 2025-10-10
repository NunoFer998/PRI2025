#!/bin/bash

# Dataset Characterization Script using csvkit and command-line tools
# This script analyzes datasets and generates CSV files with statistics

ORIGINAL_DIR="../data/original"
OUTPUT_DIR="dataset_stats"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Dataset Characterization Tool${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Check if csvkit is installed
if ! command -v csvstat &> /dev/null; then
    echo -e "${YELLOW}Warning: csvstat not found. Install with: pip install csvkit${NC}"
    echo "Proceeding with basic analysis..."
fi

analyze_docs() {
    local csv_file=$1
    
    # Check if file exists
    if [ ! -f "$csv_file" ]; then
        echo -e "${YELLOW}Error: File $csv_file not found${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Analyzing: $(basename "$csv_file")${NC}"
    echo "========================================"
    
    # Run each statistic separately
    
    echo -e "\n${GREEN}Null values:${NC}"
    csvstat "$csv_file" --nulls
    
    echo -e "\n${GREEN}Unique values:${NC}"
    csvstat "$csv_file" --unique
    
    echo -e "\n${GREEN}Most frequent values:${NC}"
    csvstat "$csv_file" --freq
    
    echo -e "\n${GREEN}Maximum value length:${NC}"
    csvstat "$csv_file" --len
    
    echo "========================================"
}

for file in "$ORIGINAL_DIR"/*.csv; do
    analyze_docs "$file"
done

echo -e "${BLUE}Analysis complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Now we will check the cleaned datasets.${NC}\n"

OUTPUT_DIR="../data/clean"
analyze_docs "$OUTPUT_DIR/final.csv"
