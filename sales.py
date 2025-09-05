from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("sales").getOrCreate()

data =  [ ("Store A", "2024-01", 800),
          ("Store A", "2024-02", 1200),
          ("Store A", "2024-03", 900),
          ("Store B", "2024-01", 1500),
          ("Store B", "2024-02", 1600),
          ("Store B", "2024-03", 1400),
          ("Store C", "2024-01", 700),
          ("Store C", "2024-02", 1000),
          ("Store C", "2024-03", 800) ]

df = spark.createDataFrame(data,["Store", "Month", "Sales"])

df_filtered = df.filter(col("sales")>=1000)

window_spec = Window.partitionBy("Store").orderBy("Month")

df_commulative = df_filtered.withColumn("CommulativeSales",sum("Sales").over(window_spec))

df_total_sales = df_commulative.groupBy("Store").agg(sum("Sales").alias("TotalSales"))

df_final = df_commulative.join(df_total_sales, "Store").select("Store","TotalSales","CommulativeSales").distinct()

df_result = df_final.orderBy(col("TotalSales").desc())

df_result.show(truncate=False)
