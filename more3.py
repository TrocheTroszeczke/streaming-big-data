import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
import logging

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
#TODO parametr
symbolsDF = spark.read.option("header", True).csv("gs://big-data-2023-ac/static_data/symbols_valid_meta.csv")
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
# wrzucić do bazy sql tak jak jest w pdfie :)

# query = groupedDF.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

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
        .mode("overwrite")
        .option("url", f"jdbc:postgresql://{host_name}:8432/streamoutput")
        .option("dbtable", "aggregations")
        .option("user", "postgres")
        .option("password", "mysecretpassword")
        .option("truncate", "true")
        .save()
)

query = streamWriter.start().awaitTermination()

# anomalie
# anomalies_window = joinedDF.withWatermark("date", "7 days") \
#     .groupBy(window("date", "7 days", "1 day"), joinedDF.symbol) \
#     .select(
#         col("Symbol"),
#         col("highest"),
#         col("lowest"),
#         date_format(col("window").start, "dd.MM.yyyy").alias("window_start"),
#         date_format(col("window").end, "dd.MM.yyyy").alias("window_end"),
#         ((col("highest") - col("lowest")) / col("highest")).alias("fluctuations_rate")
#     )
#
# anomalies = anomalies_window.where(anomalies_window.fluctuations_rate > 0.4)
# anomalies.print()
#
# #Format results for Kafka output
# anomalies_formatted = anomalies.select(concat(
#     col("window_start"),
#     lit(","),
#     col("window_end"),
#     lit(","),
#     col("Title"),
#     lit(","),
#     col("rate_count"),
#     lit(","),
#     col("avg_rate"),
# ).alias("value"))
#
# anomalies_output = anomalies_formatted.writeStream \
# .format("kafka") \
# .option("kafka.bootstrap.servers", f"{host_name}:9092") \
# .option("topic", "prj-2-anomalies") \
# .option("checkpointLocation", "/tmp/anomaly_checkpoints/") \
# .start()