from pyspark import SparkContext
import sys

# Initialize SparkContext
sc = SparkContext("local", "TreeCount")


# path = "file:///root/arbres.csv"         # For Method 1 (Local)
# path = "hdfs://master:9000/arbres.csv"   # For Method 2 (HDFS)

path = sys.argv[1]

lines_rdd = sc.textFile(path)

# Count the lines
count = lines_rdd.count()
print(f"Number of lines in the file: {count}")