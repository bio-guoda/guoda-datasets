from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Elevation Parquet Converter") \
    .getOrCreate()

df = spark.read.csv("dem3/*.csv")
df.write.parquet("elevation.parquet")
