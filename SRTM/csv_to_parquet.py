from pyspark import SparkContext, SparkConf

sc = SparkContext(appName="Elevation Parquet Converter")
sqlContext = SQLContext(sc)

df = spark.read.csv("dem3/*.csv")
df.write.parquet("elevation.parquet")
