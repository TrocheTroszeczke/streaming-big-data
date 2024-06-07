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

dataDF = valuesDF.select(
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
groupedDF = joinedDF \
        .groupBy(window("timestamp", "30 days"), "Symbol", "Security Name") \
        .agg(
            avg("Close").alias("avg_close"),
            min("Low").alias("lowest"),
            max("High").alias("highest"),
            sum("Volume").alias("sum_volume")
        )

query = groupedDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination()