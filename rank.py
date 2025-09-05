from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("rank").getOrCreate()

data = [("Alice", "Math", 90),
        ("Alice", "English", 85),
        ("Bob", "Math", 70),
        ("Bob", "English", 88)]

columns = ["name", "subject", "score"]

df = spark.createDataFrame(data,columns)

Window_spec = Window.orderBy(desc("score"))

df = df.withColumn("Rank", rank().over(Window_spec))

df.show()