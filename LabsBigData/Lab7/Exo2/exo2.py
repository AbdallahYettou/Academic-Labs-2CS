from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StringType, IntegerType
""" 
WE NEED TO DO 4 THINGS:
Listener: Open the connection to Kafka and wait for new data to arrive.

Parsing: Clean up the messy binary data and structure it into a readable JSON format.

Aggregation: Do the actual math (counting the words) while remembering the historical state.

Output: Push the freshly updated results to the console so you can see them in real-time
"""
#  1 - Create SparkSession
spark = (SparkSession.builder
         .appName("KafkaStructuredStreaming")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

#  2 - Define schema Why UPPERCASE? ksqlDB automatically converts all field names to uppercase when writing to Kafka
schema = (StructType()
          .add("TITLE_ID",    IntegerType())
          .add("CHANGE_TYPE", StringType())
          .add("CREATED_AT",  StringType()))

#  3 - Read streaming DataFrame from Kafka
raw_df = (spark.readStream #this is a streaming source, not a static batch. The resulting DataFrame is unbounded — it keeps receiving new rows forever as new Kafka messages arrive
               .format("kafka") ## the data source 
               .option("kafka.bootstrap.servers", "localhost:29092")
               .option("subscribe", "production_changes")#same Kafka topic — In Exo1 Q3
               .option("startingOffsets", "earliest")# reads from the very beginning, including all messages already in the topic 
               .load())

#  4 - Parse JSON
parsed_df = (raw_df #The actual message payload is in value as raw bytes
             .selectExpr("CAST(value AS STRING) AS json_value", "timestamp")# converts the binary bytes to a readable text string, giving us the JSON text like {"TITLE_ID":1,"CHANGE_TYPE":"season_length",...}
             .select(from_json(col("json_value"), schema).alias("data"), "timestamp")
             .select("data.*", "timestamp")
             .filter(col("CHANGE_TYPE") == "season_length"))# we only care about season length events.

#  5 - Windowed aggregation (1-hour tumbling window) Without a window, count(*) would just give you one number for the entire history of the stream — "how many season_length changes happened ever". That's not useful for monitoring.
agg_df = (parsed_df
          .groupBy(
              window(col("timestamp"), "1 hour"),
              col("TITLE_ID")
          )
          .agg(count("*").alias("change_count"))
          .select(
              col("window.start").alias("window_start"),
              col("window.end").alias("window_end"),
              col("TITLE_ID").alias("title_id"),
              col("change_count")
          ))

#  6 - Write to console
query = (agg_df.writeStream
               .format("console")
               .outputMode("complete")
               .option("truncate", False)
               .trigger(processingTime="13 seconds")
               .start())

query.awaitTermination()