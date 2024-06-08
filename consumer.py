import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
import logging
import os

def main():
    if len(sys.argv) < 4:
        print("Za mało parametrów. Musisz podać tryb (A/C), długość okna anomalii (w dniach),"
              "dopuszczalne graniczne wahanie kursu (%)")
        return

    mode = sys.argv[1]
    d = int(sys.argv[2])
    p = float(sys.argv[3])
    folder = sys.argv[4]
    print(f"Argumenty: {mode}, {d}, {p}, {folder}")

    # Sesja Spark
    spark = SparkSession.builder \
        .appName("Stock Data") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Wczytwanie danych
    # Strumień danych
    host_name = socket.gethostname()
    ds1 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host_name}:9092") \
        .option("subscribe", "kafka-input") \
        .load()
    logging.info("Schemat danych otrzymywanych strumieniem danych")
    ds1.printSchema()

    # Plik statyczny z nazwami spółek
    symbolsDF = spark.read.option("header", True).csv(folder)
    logging.info("Schemat danych statycznych")
    symbolsDF.printSchema()

    # Przygotowanie danych do agregacji: join dwóch źródeł danych
    schema = "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, Adj_close STRING, Volume DOUBLE, Stock String"

    dataDF = ds1.select(
        from_csv(col("value").cast(StringType()), schema)
        .alias("val")) \
        .select(col("val.Date"), col("val.Open"), col("val.High"), col("val.Low"), col("val.Close"),
                col("val.Adj_close"), col("val.Volume"), col("val.Stock").alias("Symbol")) \
        .withColumn("timestamp", to_timestamp("Date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    logging.info("Schemat danych otrzymywanych strumieniem danych po przetworzeniu")
    dataDF.printSchema()

    joinedDF = dataDF.join(symbolsDF, on="Symbol", how="inner")
    logging.info("Schemat danych po połączeniu dwóch źródeł danych")
    joinedDF.printSchema()

    # Agregacje
    # TODO: parametry
    groupedDF = joinedDF \
            .groupBy(window("timestamp", "30 days"), "Symbol", "Security Name") \
            .agg(
                avg("Close").alias("avg_close"),
                min("Low").alias("lowest"),
                max("High").alias("highest"),
                sum("Volume").alias("volume")
            )

    groupedDF.printSchema()

    # query = groupedDF.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start() \
    #     .awaitTermination()

    if mode == 'A':
        output_mode = "overwrite"
    elif mode == 'C':
        output_mode = "complete"
    else:
        print("Podano błędny tryb (dopuszczalne: A, C). Dane będą przetwarzane w trybie A.")
        output_mode = "overwrite"

    streamWriter = groupedDF.writeStream.outputMode("complete").foreachBatch(
        lambda batchDF, batchId:
        batchDF.select(
            col("Symbol").alias("symbol"),
            col("Security Name").alias("security_name"),
            col("window").cast("string").alias("date"),
            col("avg_close"),
            col("lowest"),
            col("highest"),
            col("volume")
        ).write
            .format("jdbc")
            .mode(output_mode)
            .option("url", f"jdbc:postgresql://{host_name}:8432/streamoutput")
            .option("dbtable", "aggregations")
            .option("user", "postgres")
            .option("password", "mysecretpassword")
            .option("truncate", "true")
            .save()
    )

    query = streamWriter.start().awaitTermination()

    # anomalie
    anomalies_window = joinedDF.withWatermark("timestamp", "7 days") \
        .groupBy(window("timestamp", "7 days", "1 day"), joinedDF.Symbol) \
        .agg(
            max("High").alias("highest"),
            min("Low").alias("lowest")) \
        .select(
            col("Symbol"),
            col("highest"),
            col("lowest"),
            date_format(col("window").start, "dd.MM.yyyy").alias("window_start"),
            date_format(col("window").end, "dd.MM.yyyy").alias("window_end"),
            ((col("highest") - col("lowest")) / col("highest")).alias("fluctuations_rate")
        )

    anomalies = (anomalies_window.where(anomalies_window.fluctuations_rate > (p / 100))
                 .select(concat(
                    col("window_start"),
                    lit(","),
                    col("window_end"),
                    lit(","),
                    col("Symbol"),
                    lit(","),
                    col("highest"),
                    lit(","),
                    col("lowest"),
                    lit(","),
                    col("fluctuations_rate"),
                 ).alias("value")))

    anomalies_output = (anomalies.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("topic", "anomalies") \
    .option("checkpointLocation", "/tmp/anomalies_checkpoints/") \
    .start().awaitTermination())

if __name__ == "__main__":
    main()

