from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, window

spark = SparkSession.builder.appName("TumblingWindowTypeCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("DATA HORA", TimestampType(), True),
    StructField("CLASSIFICAÇÃO", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic_sensor") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Group by the 5-minute window AND the classification
windowed_counts = parsed_df.groupBy(
    window(col("DATA HORA"), "5 minutes"),
    col("CLASSIFICAÇÃO")
).count()

query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
