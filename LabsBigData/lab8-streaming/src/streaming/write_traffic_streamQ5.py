from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, struct, lit, col, encode

spark = SparkSession.builder \
    .appName("WriteTrafficStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ID EQP",             LongType(),      True),
    StructField("DATA HORA",          TimestampType(), True),
    StructField("MILESEGUNDO",        LongType(),      True),
    StructField("CLASSIFICAÇÃO",      StringType(),    True),
    StructField("FAIXA",              LongType(),      True),
    StructField("ID DE ENDEREÇO",     LongType(),      True),
    StructField("VELOCIDADE DA VIA",  StringType(),    True),
    StructField("VELOCIDADE AFERIDA", StringType(),    True),
    StructField("TAMANHO",            StringType(),    True),
    StructField("NUMERO DE SÉRIE",    LongType(),      True),
    StructField("LATITUDE",           StringType(),    True),
    StructField("LONGITUDE",          StringType(),    True),
    StructField("ENDEREÇO",           StringType(),    True),
    StructField("SENTIDO",            StringType(),    True),
])

df = spark.readStream \
    .schema(schema) \
    .parquet("/data/AGOSTO_2022_PARQUET_FINAL/")

df = df.limit(500000)

# Simplest and most robust casting for Kafka
kafka_df = df.select(
    lit("key").cast("string").alias("key"),
    to_json(struct([col(c) for c in df.columns])).cast("string").alias("value")
)

query = kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "traffic_sensor") \
    .option("checkpointLocation", "/tmp/checkpoint_v2") \
    .outputMode("append") \
    .start()

query.awaitTermination()
