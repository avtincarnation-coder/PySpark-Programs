from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("price category").getOrCreate()

data = [("Laptop", 800), ("Mouse", 25), ("Keyboard", 150), ("Monitor", 300)]

columns = ["product", "price"]

df = spark.createDataFrame(data,columns)

df_price = df.withColumn("price_category",when(col("price")<100, "Low")
                         .when((col("price")>=100) & (col("price")<=500), "Medium")
                         .otherwise("High"))

df_expr = df.selectExpr("product","price","case when price<100 then 'Low' when price>=100 and price<500  then 'Medium' else 'High' end as price_category")

df_price.show(truncate=True)
df_expr.show(truncate=True)