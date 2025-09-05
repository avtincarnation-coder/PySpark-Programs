from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("rddtodf").getOrCreate()

rdd = spark.sparkContext.parallelize(
[
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Charlie", 25)
])

df = rdd.toDF(["id","name","score"])


df.show()