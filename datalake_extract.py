import os
import shutil
import pandas as pd
import requests
import zipfile
from azure.storage.blob import ContainerClient, generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta

class AzureBlobHandler:
    def __init__(self, account_name, container_name, account_key):
        self.account_name = account_name
        self.container_name = container_name
        self.account_key = account_key
        self.container_url = f"https://{account_name}.blob.core.windows.net/{container_name}"
        self.sas_token = self._generate_container_sas()

    def _generate_container_sas(self):
        return generate_container_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            account_key=self.account_key,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
    #récupère la liste des blobs d'un répertoire donné
    def list_blobs(self, target_dir):
        container_client = ContainerClient.from_container_url(f"{self.container_url}?{self.sas_token}")
        blobs = container_client.list_blobs(name_starts_with=target_dir)
        return [blob.name for blob in blobs]

    #télécharge un blob spécifique
    def download_blob(self, blob_name, output_dir):
        blob_url = f"{self.container_url}/{blob_name}?{self.sas_token}"
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

    #traitement des fichiers parquet, extrait img et genere csv avec titre et chemin img
    def process_parquet(self, file_path, base_output_dir):
        parquet_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join(base_output_dir, "product_eval", parquet_name)
        os.makedirs(output_dir, exist_ok=True)

        df = pd.read_parquet(file_path, engine='pyarrow')
        print(f"Traitement du fichier : {file_path}")

        image_dir = os.path.join(output_dir, "images")
        os.makedirs(image_dir, exist_ok=True)
        image_paths = []

        for index, row in df.iterrows():
            image_data = row['image']['bytes']
            image_path = os.path.join(image_dir, f"image_{index}.png")
            with open(image_path, "wb") as img_file:
                img_file.write(image_data)
            image_paths.append(image_path)
            print(f"Image sauvegardée : {image_path}")

        df['image_path'] = image_paths
        csv_path = os.path.join(output_dir, f"{parquet_name}_titles_and_images.csv")
        df[['title', 'image_path']].to_csv(csv_path, index=False)
        print(f"Titres et chemins d'images sauvegardés dans : {csv_path}")

    #télécharge tous les csv dans un répertoire et ses sous dossier
    def process_csv(self, target_dir, base_output_dir):
        output_dir = os.path.join(base_output_dir, "nlp_data")
        os.makedirs(output_dir, exist_ok=True)
        blobs = self.list_blobs(target_dir)
        for blob_name in blobs:
            if blob_name.endswith('.csv'):
                file_path = self.download_blob(blob_name, output_dir)
                print(f"CSV téléchargé : {file_path}")

    #telecharge et extrait des fichiers zip
    def process_zip(self, blob_name, base_output_dir):
        output_dir = os.path.join(base_output_dir, "machine_learning")
        os.makedirs(output_dir, exist_ok=True)
        zip_path = self.download_blob(blob_name, output_dir)
        if zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            print(f"ZIP extrait dans : {output_dir}")
        else:
            print(f"Le fichier téléchargé n'est pas un ZIP valide : {zip_path}")

    #methode de nettoyage
    def cleanup_files(self, base_output_dir, extensions_to_remove):
        for root, _, files in os.walk(base_output_dir):
            for file in files:
                if any(file.endswith(ext) for ext in extensions_to_remove):
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    print(f"Fichier supprimé : {file_path}")

    #identifie le type de contenu (parquet,zip,csv...) et applique ttt approprié
    def process_directory(self, target_dir, base_output_dir):
        if target_dir == "product_eval":
            blobs = self.list_blobs(target_dir)
            for blob_name in blobs:
                if blob_name.endswith('.parquet'):
                    file_path = self.download_blob(blob_name, base_output_dir)
                    self.process_parquet(file_path, base_output_dir)
        elif target_dir == "nlp_data":
            self.process_csv(target_dir, base_output_dir)
        elif target_dir == "machine_learning":
            blobs = self.list_blobs(target_dir)
            for blob_name in blobs:
                if blob_name.endswith('.zip'):
                    self.process_zip(blob_name, base_output_dir)

        
        self.cleanup_files(base_output_dir, ['.parquet', '.zip'])


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    container_name = os.getenv('BLOB_CONTAINER')
    account_key = os.getenv('ACCOUNT_KEY')

    handler = AzureBlobHandler(account_name, container_name, account_key)

    # processing répertoire
    base_output_dir = "telechargements"
    handler.process_directory("product_eval", base_output_dir)
    handler.process_directory("nlp_data", base_output_dir)
    handler.process_directory("machine_learning", base_output_dir)
