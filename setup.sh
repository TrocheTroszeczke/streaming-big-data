#!/bin/bash

# Zakładam, że pliki znajdują się w buckecie w folderze wskazanym parametrem
# (dla strumienia znajdują się w podfolderze data)

# Ustaw zmienne środowiskowe (w tym parametry)
source ./env.sh
echo "Ustwiono wartości zmiennych środowiskowych."

# TODO Dodanie repozytorium sbt
#echo "Adding sbt repository..."
#echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
#echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
#echo "Sbt repository added successfully."

# Pobieranie sterownika JDBC
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
echo "Pobrano sterownik JDBC."

# TODO Aktualizacja listy pakietów
#echo "Updating package lists..."
#curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
#sudo apt-get update
#echo "Package lists updated successfully."

# todo
#echo "Installing sbt."
#sudo apt-get install sbt
#echo "Sbt installed successfully."

#TODO
#echo "Building code sources."
#sbt clean assembly
#echo "Project built successfully."

# Tworzenie katalogu dla danych wejściowych
#echo "Creating directory for input data..."
#mkdir "$INPUT_DIRECTORY_PATH"
#echo "Input data directory created successfully."

# Pobieranie plików z GCS
#echo "Copying input files from GCS..."
#hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/netflix-prize-data/*.csv "$INPUT_DIRECTORY_PATH"
#echo "Input files copied successfully."

# Tworzenie tematów Kafka
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --create \
--bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
--replication-factor 2 --partitions 3 --topic kafka-input
echo "Utworzono temat Kafka."

# Uruchomienie kontenera Docker z bazą danych PostgreSQL
docker run --name postgresdb \
-p 8432:5432 \
-e POSTGRES_PASSWORD=mysecretpassword \
-d postgres
sleep 10
echo "Uruchomiono kontener Docker z bazą danych PostgreSQL."

# Wykonanie skryptu SQL
psql -h localhost -p 8432 -U postgres -v user="user" -v password="mysecretpassword" -v db_name="$JDBC_DATABASE" -f setup.sql
echo "Przygotowano bazę danych PostgreSQL."