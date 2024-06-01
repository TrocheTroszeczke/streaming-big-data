import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder \
    .appName("Stock Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

host_name = socket.gethostname()
ds1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("subscribe", "kafka-input") \
    .load()

ds1.printSchema()

valuesDF = ds1.select(expr("CAST(value AS STRING)").alias("value"))

schema = "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, Adj_close STRING, Volume LONG, Stock String"

dataDF = valuesDF.select(
    from_csv(col("value").cast(StringType()), schema)
    .alias("val")) \
    .select(col("val.Date"), col("val.Open"), col("val.High"), col("val.Low"), col("val.Close"),
            col("val.Adj_close"), col("val.Volume"), col("val.Stock")) \
    .withColumn("timestamp", to_timestamp("Date", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# Jeśli simple_in_out.py działa, to puścić to - cała schema csv zdefiniowana i groupby
# TODO: join z nazwą (statycznym plikiem)
groupedDF = dataDF \
        .groupBy(window("timestamp", "30 days"), "Stock") \
        .agg(
            avg("Close").alias("avg_close"),
            min("Low").alias("lowest"),
            max("High").alias("Highest"),
            sum("Volume").alias("sum_volume")
        )

# TODO: powinno wyprintować do consoli :) mój ty pongliszu :( Widać, że poznoniok

# resultDF = dataDF.groupBy("house").agg(count("score").alias("how_many"), sum("score").alias("sum_score"),
#                                        approx_count_distinct("character", 0.1).alias("no_characters"))

dataDF.printSchema()

query = groupedDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination()
