from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("product").getOrCreate()

data = [ (1, "Laptop", 1000, 5),
         (2, "Mouse", None, None),
         (3, "Keyboard", 50, 2),
         (4, "Monitor", 200, None),
         (5, None, 500, None) ]

columns =  ["product_id", "product", "price", "quantity"]

df = spark.createDataFrame(data,columns)

mean_price_row  = df.select(mean("price")).first()

mean_price = mean_price_row[0]

df_filled = df.fillna({"price": mean_price})

df_notnull = df_filled.filter(col("product").isNotNull())

df_one = df_notnull.fillna({"quantity":1})





















