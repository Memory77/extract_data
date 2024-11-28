#!/bin/bash

# Activer le mode strict pour arrêter le script en cas d'erreur
set -e

echo "Lancement du script d'extraction de la base de données..."
python3 /app/db_extract.py || echo "Erreur : lors de l'extraction des données de la base de données"

echo "Lancement du script d'extraction du DataLake..."
python3 /app/datalake_extract.py || echo "Erreur : lors de l'extraction des données du DataLake"

echo "Tous les processus sont terminés."
