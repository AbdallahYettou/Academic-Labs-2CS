from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder.appName("CountVehiclesByType").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# We only need to define the schema for the column we care about
json_schema = StructType([
    StructField("CLASSIFICAÇÃO", StringType(), True)
])

# 1. Read the stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic_sensor") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Convert the Kafka binary 'value' to a String, then parse the JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# 3. Group by the classification and count
counts = parsed_df.groupBy("CLASSIFICAÇÃO").count()

# 4. Write the continuously updating results to the console
query = counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
