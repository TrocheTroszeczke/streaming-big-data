import socket
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder \
    .appName("Stock Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Strumień danych
host_name = socket.gethostname()
ds1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("subscribe", "kafka-input") \
    .load()

ds1.printSchema()

# Plik statyczny z nazwami spółek
# symbolsDF = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("symbols_valid_meta.csv")
#TODO parametr
symbolsDF = spark.read.option("header", True).csv("gs://pojemnik/projekt2/movie_titles.csv")
symbolsDF.printSchema()

valuesDF = ds1.select(expr("CAST(value AS STRING)").alias("value"))

schema = "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, Adj_close STRING, Volume DOUBLE, Stock String"

dataDF = valuesDF.select(
    from_csv(col("value").cast(StringType()), schema)
    .alias("val")) \
    .select(col("val.Date"), col("val.Open"), col("val.High"), col("val.Low"), col("val.Close"),
            col("val.Adj_close"), col("val.Volume"), col("val.Stock")) \
    .withColumn("timestamp", to_timestamp("Date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

joinedDF = dataDF.join(symbolsDF, on="Stock", how="inner")

# Jeśli simple_in_out.py działa, to puścić to - cała schema csv zdefiniowana i groupby
# TODO: join z nazwą (statycznym plikiem)
groupedDF = dataDF \
        .groupBy(window("timestamp", "30 days"), "Stock", "CompanyName") \
        .agg(
            avg("Close").alias("avg_close"),
            min("Low").alias("lowest"),
            max("High").alias("highest"),
            sum("Volume").alias("sum_volume")
        )

# TODO: powinno wyprintować do consoli

# resultDF = dataDF.groupBy("house").agg(count("score").alias("how_many"), sum("score").alias("sum_score"),
#                                        approx_count_distinct("character", 0.1).alias("no_characters"))

groupedDF.printSchema()
# wrzucić do bazy sql tak jak jest w pdfie :)

# query = groupedDF.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

streamWriter = groupedDF.writeStream.outputMode("complete").foreachBatch(
    lambda batchDF, batchId:
    batchDF.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", f"jdbc:postgresql://{host_name}:8432/streamoutput")
        .option("dbtable", "housestats")
        .option("user", "postgres")
        .option("password", "mysecretpassword")
        .option("truncate", "true")
        .save()
)

query = streamWriter.start()