from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, window

spark = SparkSession.builder.appName("SlidingWindowCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# We just need the timestamp for the total count
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

# Group by a 30-minute sliding window that updates every 5 minutes
sliding_counts = parsed_df.groupBy(
    window(col("DATA HORA"), "30 minutes", "5 minutes")
).count()

# Write to console (truncate=false is required here!)
query = sliding_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
