import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, struct, lit, col

spark = SparkSession.builder.appName("WriteTrafficBatch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ID EQP", LongType(), True),
    StructField("DATA HORA", TimestampType(), True),
    StructField("MILESEGUNDO", LongType(), True),
    StructField("CLASSIFICAÇÃO", StringType(), True),
    StructField("FAIXA", LongType(), True),
    StructField("ID DE ENDEREÇO", LongType(), True),
    StructField("VELOCIDADE DA VIA", StringType(), True),
    StructField("VELOCIDADE AFERIDA", StringType(), True),
    StructField("TAMANHO", StringType(), True),
    StructField("NUMERO DE SÉRIE", LongType(), True),
    StructField("LATITUDE", StringType(), True),
    StructField("LONGITUDE", StringType(), True),
    StructField("ENDEREÇO", StringType(), True),
    StructField("SENTIDO", StringType(), True),
])

# 1. Batch Read (read instead of readStream)
# Limit to 20 records so it doesn't run forever
df = spark.read \
    .schema(schema) \
    .parquet("/data/AGOSTO_2022_PARQUET_FINAL/") \
    .limit(20)

# 2. Format for Kafka
kafka_df = df.select(
    lit("key").cast("string").alias("key"),
    to_json(struct([col(c) for c in df.columns])).cast("string").alias("value")
)

# Bring the records to the Driver to iterate one by one
records = kafka_df.collect()

# 3. Process one record at a time with a delay
for row in records:
    single_row_df = spark.createDataFrame([row], schema=kafka_df.schema)
    
    # 4. Write using batch .write (not writeStream)
    single_row_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "traffic_sensor") \
        .save()
        
    print("Successfully sent 1 record! Waiting 2.5 seconds...")
    time.sleep(2.5)
