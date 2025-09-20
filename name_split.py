from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("split name").getOrCreate()

data = [(1,"Joe Root"),
(2,"Virat Kohli"),
(3,"Rohit Sharma")]

df = spark.createDataFrame(data,["id","Full Name"])

df_split = df.withColumn("first name",split(df["Full Name"]," ")[0]).withColumn("Last Name",split(df["Full Name"]," ")[1])

df_split.show()