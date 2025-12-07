#!/bin/bash

echo "=========================================="
echo " M3: Advanced Experiments"
echo "=========================================="

# Activer l'environnement
source ~/miniconda3/etc/profile.d/conda.sh
conda activate bda-env

cd ~/bda-project

# Étape 1: Créer les features avancées
echo ""
echo " Step 1/2: Creating advanced features..."
python src/advanced_features.py

# Étape 2: Lancer les expérimentations
echo ""
echo " Step 2/2: Running advanced model experiments..."
python src/advanced_models.py

echo ""
echo " M3 completed!"
echo "Check results in:"
echo "  - logs/project_metrics_log.csv"
echo "  - Compare baseline vs advanced models"

