## Jak uruchomić program?

1. Uruchom klaster, załaduj potrzebne dane do bucketa i w pliku consumer.sh ustaw wartość parametrów. Dane zasilające strumień powinny znajdować się w folderze data i widoczne po uruchomieniu polecenia `ls data`

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

3. Uruchom program konsumenta:

``source ./consumer.sh``

4. W oddzielnym terminalu uruchom program producenta:

``source ./producer.sh``
