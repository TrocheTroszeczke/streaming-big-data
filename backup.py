import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder \
    .appName("Stock Data") \
    .getOrCreate()

host_name = socket.gethostname()
ds1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("subscribe", "kafka-input") \
    .load()

ds1.printSchema()

valuesDF = ds1.select(expr("CAST(value AS STRING)").alias("value"))

schema = "Date STRING, Open STRING, High STRING, Low STRING"

# dataDF = valuesDF.select(
#     from_csv(col("value").cast(StringType()), schema)
#     .alias("val")) \
#     .select(col("val.house"), col("val.character"),
#             col("val.score").cast("int").alias("score"), col("val.ts"))

dataDF = valuesDF.select(
    from_csv(col("value").cast(StringType()), schema))

resultDF = dataDF.groupBy("house").agg(count("score").alias("how_many"), sum("score").alias("sum_score"),
                                       approx_count_distinct("character", 0.1).alias("no_characters"))

resultDF.printSchema()

query = resultDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
