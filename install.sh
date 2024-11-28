#!/bin/bash

# Se positionner dans le répertoire du script
cd "$(dirname "$0")"

# Vérifier si .env existe
if [[ ! -f ".env" ]]; then
    echo "Le fichier .env est manquant ! Renseignez le .env"
    exit 1
fi




if ! [[ "18.04 20.04 22.04 23.04 24.04" == *"$(lsb_release -rs)"* ]];
then
    echo "Ubuntu $(lsb_release -rs) is not currently supported.";
    exit;
fi

echo 'installation de ODBC'
# Add the signature to trust the Microsoft repo
# For Ubuntu versions < 24.04 
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
# For Ubuntu versions >= 24.04
curl https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Add repo to apt sources
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Install the driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
# optional: for unixODBC development headers
sudo apt-get install -y unixodbc-dev


echo 'Installation du venv'
#installation du venv
python3 -m venv venv

echo 'Activation du venv'
#entrer dans le venv
source venv/bin/activate

echo 'upgrade pip'
python3 -m pip install --upgrade pip

#installation des dépendances
echo 'installation des dépendances'
pip install -r requirements.txt


#lancement du script
echo "lancement du script d'extraction de la base de donnée"
python3 db_extract.py || echo "Erreur : lors de l'extraction des données de la bdd"

echo "lancement du script d'extraction du datalake..."
python3 datalake_extract.py || echo "Erreur : lors de l'extraction des données du datalake"
