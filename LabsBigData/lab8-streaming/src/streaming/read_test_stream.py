from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadTestStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "earliest") \
    .load()

decoded = df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
)

query = decoded.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
