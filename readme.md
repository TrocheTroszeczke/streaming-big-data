## Jak uruchomić program?

1. Uruchom klaster, załaduj statyczne dane do bucketa i w pliku consumer.sh ustaw wartość parametrów. Dane zasilające strumień powinny znajdować się w folderze data i być widoczne po uruchomieniu polecenia `ls data`

``gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components JUPYTER,ZOOKEEPER,DOCKER \
--project ${PROJECT_ID} --max-age=3h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh``

2. Uruchom skrypt konfiguracyjny:

``source ./setup.sh``

Po pytaniu o hasło należy wpisać: mysecretpassword

3. Uruchom program konsumenta:

``source ./consumer.sh``

4. W oddzielnym terminalu uruchom program producenta:

``source ./producer.sh``

5. Żeby odczytać przetworzone dane, przejdź do terminala PostgreSQL i wypisz zawartość:

``export PGPASSWORD='mysecretpassword'
psql -h localhost -p 8432 -U postgres``

``\c streamoutput;``

``select * from aggregations;``

6. Żeby odczytać dane o anomaliach, odczytaj dane z tematu Kafka `anomalies`, 
np. za pomocą konsumenta wypisującego zawartość na konsolę:

``kafka-console-consumer.sh --group my-consumer-group \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --topic anomalies --from-beginning``

7. Po zakończeniu programu, uruchom skrypt czyszczący środowisko:

``source ./cleanup.sh``