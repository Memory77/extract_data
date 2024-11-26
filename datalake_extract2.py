import requests
import os
import pandas as pd
from azure.storage.blob import ContainerClient, generate_container_sas, ContainerSasPermissions
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Charger les variables d'environnement
load_dotenv()

# Récupérer les valeurs des variables d'environnement
storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
container_name = os.getenv('BLOB_CONTAINER')
target_dir = os.getenv('TARGET_DIR')  # Exemple : "dossier_cible/"
account_key = os.getenv('ACCOUNT_KEY')

# Générer une URL SAS pour le conteneur
def generate_container_sas_url():
    sas_token = generate_container_sas(
        account_name=storage_account_name,
        container_name=container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    container_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}"
    return f"{container_url}?{sas_token}"

# Lister les blobs du conteneur
def list_blobs(container_sas_url, target_dir):
    container_client = ContainerClient.from_container_url(container_sas_url)
    blobs = container_client.list_blobs(name_starts_with=target_dir)
    return [blob.name for blob in blobs]

# Télécharger un blob avec HTTP GET
def download_blob_via_get(container_url, blob_name, sas_token, output_dir):
    blob_url = f"{container_url}/{blob_name}?{sas_token}"
    response = requests.get(blob_url, stream=True)
    if response.status_code == 200:
        output_path = os.path.join(output_dir, os.path.basename(blob_name))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Blob téléchargé : {output_path}")
        return output_path
    else:
        print(f"Erreur lors du téléchargement du blob {blob_name}: {response.status_code}, {response.text}")
        return None

# Transformer le fichier Parquet en PNG et CSV
def process_parquet(file_path, base_output_dir):
    # Identifier un sous-dossier unique pour ce fichier Parquet
    parquet_name = os.path.splitext(os.path.basename(file_path))[0]
    output_dir = os.path.join(base_output_dir, parquet_name)
    os.makedirs(output_dir, exist_ok=True)

    # Lire le fichier Parquet
    df = pd.read_parquet(file_path, engine='pyarrow')
    print(f"Traitement du fichier : {file_path}")

    # Créer une nouvelle colonne pour le chemin des images
    image_dir = os.path.join(output_dir, "images")
    os.makedirs(image_dir, exist_ok=True)
    image_paths = []

    # Extraire les images et sauvegarder en PNG
    for index, row in df.iterrows():
        image_data = row['image']['bytes']  # Accéder aux données binaires de l'image
        image_path = os.path.join(image_dir, f"image_{index}.png")
        with open(image_path, "wb") as img_file:
            img_file.write(image_data)
        image_paths.append(image_path)  # Enregistrer le chemin de l'image pour le CSV
        print(f"Image sauvegardée : {image_path}")

    # Ajouter les chemins des images au DataFrame
    df['image_path'] = image_paths

    # Sauvegarder le DataFrame mis à jour (avec les titres et les chemins d'images) en CSV
    csv_path = os.path.join(output_dir, f"{parquet_name}_titles_and_images.csv")
    df[['title', 'image_path']].to_csv(csv_path, index=False)
    print(f"Titres et chemins d'images sauvegardés dans : {csv_path}")

# Script principal
if __name__ == "__main__":
    # Générer l'URL SAS pour le conteneur
    container_sas_url = generate_container_sas_url()
    print(f"URL SAS du conteneur : {container_sas_url}")

    # Lister les blobs dans le répertoire cible
    blobs = list_blobs(container_sas_url, target_dir)
    print(f"Blobs trouvés dans {target_dir} : {blobs}")

    # Télécharger chaque blob
    base_output_dir = "downloads"
    for blob_name in blobs:
        file_path = download_blob_via_get(
            container_url=f"https://{storage_account_name}.blob.core.windows.net/{container_name}",
            blob_name=blob_name,
            sas_token=container_sas_url.split('?')[-1],
            output_dir=base_output_dir
        )

        # Si le fichier téléchargé est un Parquet, le traiter
        if file_path and file_path.endswith('.parquet'):
            process_parquet(file_path, base_output_dir)
