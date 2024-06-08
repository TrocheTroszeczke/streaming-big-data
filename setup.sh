#!/bin/bash

# Zakładam, że pliki znajdują się w buckecie w folderze wskazanym parametrem
# (dla strumienia znajdują się w podfolderze data)

# Ustaw zmienne środowiskowe (parametry)
source ./env.sh
echo "Ustwiono wartości zmiennych środowiskowych."

# Pobieranie sterownika JDBC
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
echo "Pobrano sterownik JDBC."

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