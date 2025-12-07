#!/bin/bash

echo "=========================================="
echo " M2: ETL + Baseline"
echo "=========================================="

# Activer l'environnement
source ~/miniconda3/etc/profile.d/conda.sh
conda activate bda-env

cd ~/bda-project

# Étape 1: Parser les blocs
echo ""
echo " Step 1/3: Parsing Bitcoin blocks..."
python src/parsers/bitcoin_parser.py

# Étape 2: ETL pipeline
echo ""
echo " Step 2/3: Running ETL pipeline..."
python src/etl_pipeline.py

# Étape 3: Train baseline
echo ""
echo " Step 3/3: Training baseline model..."
python src/baseline_model.py

echo ""
echo " M2 completed!"
echo "Check results in:"
echo "  - data/processed/"
echo "  - models/baseline_model/"
echo "  - logs/project_metrics_log.csv"
echo "  - evidence/"

