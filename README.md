
<strong>Description</strong>

extract_data est un projet qui permet d'extraire des données depuis trois sources différentes :

- Une base de données
- Un fichier de données
- Un système Big Data

<img width="1800" height="3000" alt="Untitled diagram _ Mermaid Chart-2025-08-14-154136" src="https://github.com/user-attachments/assets/b05dcaec-053b-4ae8-9157-89d5e14f8b40" />


Le projet utilise Docker pour faciliter l'exécution sur n'importe quel système d'exploitation.


<strong>Prérequis</strong>

Docker installé sur votre machine.
Git pour cloner le dépôt.
    

<strong>Installation</strong>

Cloner le dépôt :

git clone https://github.com/Memory77/extract_data.git
cd extract_data

Configurer la planification (facultatif) :

Si vous souhaitez exécuter le script à intervalles réguliers, ouvrez le fichier cron_docker et ajustez la planification à votre convenance :

    0 * * * * /scripts_docker.sh >> /var/log/cron.log 2>&1

Construire l'image Docker :

    docker build -t extract_data_image .
    

<strong>Exécution</strong>

Exécution unique du conteneur :

    docker run --rm extract_data_image

Cela exécute le projet une seule fois, puis supprime automatiquement le conteneur.

Exécution en arrière-plan (mode détaché) :

Si vous voulez que le conteneur tourne en arrière-plan :

    docker run -d --name extract_data_container extract_data_image

Vous pouvez vérifier les logs avec :

    docker logs -f extract_data_container

Et arrêter le conteneur avec :

    docker stop extract_data_container
    

<strong>Structure du projet</strong>

- datalake_extract.py : Script principal pour l'extraction des données.
- db_extract.py : Script pour l'extraction des données depuis une base de données.
- dockerfile : Fichier Docker pour construire l'image.
- cron_docker : Configuration cron pour exécuter le script à intervalles réguliers.
- scripts_docker.sh : Script exécuté par cron pour lancer l'extraction.
