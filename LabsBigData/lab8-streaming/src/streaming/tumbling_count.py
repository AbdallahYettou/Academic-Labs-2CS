from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, window

spark = SparkSession.builder.appName("TumblingWindowCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# We only need the timestamp column for this question
schema = StructType([
    StructField("DATA HORA", TimestampType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic_sensor") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Group by a 5-minute tumbling window on the timestamp column
windowed_counts = parsed_df.groupBy(
    window(col("DATA HORA"), "1 minutes")
).count()

# Write to console
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
