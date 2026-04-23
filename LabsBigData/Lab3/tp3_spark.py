import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # 1. Initialize SparkSession (Cluster Mode)
    spark = SparkSession.builder \
        .appName("TP3_Ngrams_Execution") \
        .getOrCreate()

    # 2. IMPORTANT: Change this path to where your file is on the cluster!
    # If using HDFS, it will look like 'hdfs://namenode:8020/user/hadoop/ngram.csv'
    # If using local cluster storage, it will look like 'file:///home/user/ngram.csv'
    pyspark_file_path = 'hdfs://hadoop-master:9000/user/root/ngram.csv'

    # 3. Define Schema
    schema = StructType([
        StructField("ngram", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Count", IntegerType(), True),
        StructField("Pages", IntegerType(), True),
        StructField("Books", IntegerType(), True)
    ])

    # 4. Load Data
    print("--- LOADING DATA ---")
    pyspark_df = spark.read.csv(pyspark_file_path, sep='\t', header=False, schema=schema)
    pyspark_df.createOrReplaceTempView("ngram_table")
    pyspark_df.show(5)

    # 5. Queries (SQL and DataFrame API)
    print("--- QUERY 1: Count > 5 ---")
    spark.sql("SELECT * FROM ngram_table WHERE Count > 5").show()
    pyspark_df.filter(pyspark_df.Count > 5).show()

    print("--- QUERY 2: Total Count by Year ---")
    spark.sql("SELECT Year, SUM(Count) AS Total_Count FROM ngram_table GROUP BY Year").show()
    pyspark_df.groupBy("Year").agg(sum("Count").alias("Total_Count")).show()

    print("--- QUERY 3: Highest Count by Year ---")
    spark.sql("SELECT Year, MAX(Count) AS highest_Count FROM ngram_table GROUP BY Year").show()
    pyspark_df.groupBy("Year").agg(max("Count").alias("highest_Count")).show()

    print("--- QUERY 4: Ngrams appearing in 20 distinct years ---")
    spark.sql(""" 
        SELECT ngram, COUNT(DISTINCT Year) AS Year_Count  
        FROM ngram_table
        GROUP BY ngram  
        HAVING Year_Count = 20
    """).show()
    
    pyspark_df.groupBy("Ngram") \
        .agg(countDistinct("Year").alias("Year_Count")) \
        .filter("Year_Count == 20").distinct().show()

    print("--- QUERY 5: Ngrams with ! and 9 ---")
    spark.sql("SELECT DISTINCT ngram FROM ngram_table WHERE ngram LIKE '!% %9%'").show()
    pyspark_df.filter("ngram LIKE '!% %9%'").select("ngram").distinct().show()

    print("--- QUERY 6: Ngrams in ALL years ---")
    spark.sql("""
        SELECT Ngram , COUNT(DISTINCT Year) AS Year_Count
        FROM ngram_table
        GROUP BY Ngram
        HAVING Year_Count = (SELECT COUNT(DISTINCT Year) FROM ngram_table)
    """).show()

    total_years_count = pyspark_df.select("Year").distinct().count()
    pyspark_df.groupBy("Ngram") \
        .agg(countDistinct("Year").alias("years_count")) \
        .filter("years_count == {}".format(total_years_count)).distinct().show()

    print("--- QUERY 7: Total pages and books by bigram/year ---")
    spark.sql("""
        SELECT Ngram, Year, SUM(Pages) AS Total_Pages, SUM(Books) AS Total_Books
        FROM ngram_table
        GROUP BY Ngram, Year
        ORDER BY Ngram ASC
    """).show()

    pyspark_df.groupBy("Ngram", "Year") \
        .agg(sum("Pages").alias("Total_Pages"), sum("Books").alias("Total_Books")) \
        .orderBy(col("Ngram").asc()).show()

    print("--- QUERY 8: Distinct bigrams by year DESC ---")
    spark.sql("""
        SELECT Year, COUNT(DISTINCT Ngram) AS Distinct_Bigram_Count
        FROM ngram_table
        GROUP BY Year
        ORDER BY Year DESC
    """).show()

    pyspark_df.groupBy("Year") \
        .agg(countDistinct("Ngram").alias("Distinct_Bigram_Count")) \
        .orderBy(col("Year").desc()).show()

    # Stop the Spark session when done
    spark.stop()

if __name__ == "__main__":
    main()