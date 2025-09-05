from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("First Pyspark Program").getOrCreate()

df = spark.read.format("csv").option("header","true").load("D:/java/employees.csv")


df1 = df.select("DEPARTMENT_ID").groupBy(col("DEPARTMENT_ID").alias("Department")).count()


df1.show()











