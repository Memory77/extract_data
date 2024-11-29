# utilisation d'une image de base ubuntu
FROM ubuntu:22.04

# installation dépendances système 
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc-dev \
    software-properties-common \
    python3 \
    python3-pip \
    cron \
    && apt-get clean

# Configurer le dépôt Microsoft pour ODBC 18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18

# Ajouter mssql-tools au PATH
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
ENV PATH="$PATH:/opt/mssql-tools18/bin"

# configurer le répertoire de travail
WORKDIR /app

# copier les fichiers nécessaires spécifiques dans le repertoire travail
COPY db_extract.py /app/
COPY datalake_extract.py /app/
COPY scripts_docker.sh /app/
COPY requirements.txt /app/
COPY .env /app/

# installer les dépendances python
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# configuration du cron
COPY cron_docker /etc/cron.d/cron_docker
RUN chmod 0644 /etc/cron.d/cron_docker
RUN crontab /etc/cron.d/cron_docker

# rendre le script bash est exécutable
RUN chmod +x scripts_docker.sh

# commande principale pour démarrer le cron
CMD ["cron", "-f"]
