#!/bin/bash

docker stop postgresdb
docker rm postgresdb

kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic kafka-input
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic anomalies
