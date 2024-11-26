import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer les valeurs des variables d'environnement
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
driver = os.getenv('DRIVER')

# Connexion à la base de données
connection_string = f"Driver={driver};Server=tcp:{server};Database={database};Uid=jvcb;Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
conn = pyodbc.connect(connection_string)

# Créer un dossier pour stocker les CSV s'il n'existe pas
output_dir = "tables_csv"
os.makedirs(output_dir, exist_ok=True)

if conn:
    print("Connexion réussie.")

    # Récupérer la liste des tables des schémas 'Person' et 'Production'
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA IN ('Person', 'Production', 'Sales');
    """)
    tables = cursor.fetchall()

    # Exporter chaque table dans un fichier CSV
    for table_schema, table_name in tables:
        try:
            print(f"Exportation de la table {table_schema}.{table_name}...")
            query = f"SELECT * FROM {table_schema}.{table_name}"
            data = pd.read_sql_query(query, conn)

            # Supprimer automatiquement les colonnes problématiques
            for col in data.columns:
                try:
                    data[col].to_string()
                except Exception:
                    print(f"Exclusion de la colonne non supportée: {col}")
                    data = data.drop(columns=[col])

            # Exporter vers CSV
            csv_path = os.path.join(output_dir, f"{table_schema}_{table_name}.csv")
            data.to_csv(csv_path, index=False)
            print(f"Table {table_schema}.{table_name} exportée avec succès vers {csv_path}.")
        except Exception as e:
            print(f"Erreur lors de l'exportation de la table {table_schema}.{table_name}: {e}")

# Fermer la connexion
conn.close()
print("Connexion fermée.")
