from pyspark.sql import SparkSession
from pyspark.sql.functions import *



spark = SparkSession.builder.appName("pivot").getOrCreate()

data = [("A", "Apple,Mango,Orange"),
        ("B", "Apple"),
        ("C","Guava,Cherry"),
        ("D","Mango,Cherry,Orange")]

columns = ["Person","Basket"]

df = spark.createDataFrame(data,columns)


df_split = df.withColumn("Basket",split(df["Basket"],","))

df_exploded = df_split.withColumn("Basket",explode("Basket").alias("Basket"))

df_final = df_exploded.groupBy("Person").pivot("Basket").count().orderBy("Person")







