from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("product category").getOrCreate()

data = [ ("A", "p1", 100), ("A", "p2", 200), ("A", "p3", 200), ("B", "p4", 300), ("B", "p5", 150), ("B", "p6", 150), ("C", "p7", 400), ("C", "p8", 300), ("C", "p9", 200), ]

columns = ["category", "product", "amount"]

df = spark.createDataFrame(data,columns)



