CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--driver-class-path postgresql-42.6.0.jar \
--jars postgresql-42.6.0.jar consumer.py "$MODE" "$D" "$P" "$JDBC_DATABASE"