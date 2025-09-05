from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Welcome Pyspark").getOrCreate()

print("Welcome Pyspark")