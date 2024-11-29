import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# configuration d'un logger
log_file = "log_db_extract.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger()

# chargement des variables d'environnement
load_dotenv()

# recup les valeurs des variables d'environnement
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
driver = os.getenv('DRIVER')

# connexion à la base de données
connection_string = f"Driver={driver};Server=tcp:{server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
try:
    conn = pyodbc.connect(connection_string)
    logger.info("Connexion réussie à la base de données.")
except Exception as e:
    logger.error(f"Erreur de connexion à la base de données: {e}")
    exit(1)

# créer un dossier pour stocker les csv s'il n'existe pas
output_dir = "tables_csv"
os.makedirs(output_dir, exist_ok=True)
logger.info(f"Dossier de sortie créé: {output_dir}")

# exportation des tables
try:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA IN ('Person', 'Production', 'Sales');
    """)
    tables = cursor.fetchall()
    for table_schema, table_name in tables:
        try:
            logger.info(f"Exportation de la table {table_schema}.{table_name}...")
            query = f"SELECT * FROM {table_schema}.{table_name}"
            data = pd.read_sql_query(query, conn)
            # suppression automatiquement les colonnes problématiques
            for col in data.columns:
                try:
                    data[col].to_string()
                except Exception:
                    logger.warning(f"Exclusion de la colonne non supportée: {col}")
                    data = data.drop(columns=[col])

            # exporter csv
            csv_path = os.path.join(output_dir, f"{table_schema}_{table_name}.csv")
            data.to_csv(csv_path, index=False)
            logger.info(f"Table {table_schema}.{table_name} exportée avec succès vers {csv_path}.")
        except Exception as e:
            logger.error(f"Erreur lors de l'exportation de la table {table_schema}.{table_name}: {e}")
except Exception as e:
    logger.error(f"Erreur lors de la récupération des tables: {e}")
finally:
    # fermer la connexion
    conn.close()
    logger.info("Connexion fermée.")
