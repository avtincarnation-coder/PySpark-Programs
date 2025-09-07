from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("dataset transaction").getOrCreate()

data = [ (1, 101, 500.0, "2024-01-01"), (2, 102, 200.0, "2024-01-02"),
(3, 101, 300.0, "2024-01-03"), (4, 103, 100.0, "2024-01-04"),
(5, 102, 400.0, "2024-01-05"), (6, 103, 600.0, "2024-01-06"),
(7, 101, 200.0, "2024-01-07"), ]

columns =  ["transaction_id", "user_id", "transaction_amount", "transaction_date"]

df = spark.createDataFrame(data,columns)

df_total = df.groupBy("user_id").agg(sum("transaction_amount").alias("txn_amt"))

window_spec = Window.orderBy(col("txn_amt").desc())

df_topusers = df_total.withColumn("rank", rank().over(window_spec)).filter(col("rank")<=3).drop("rank")

df_topthree = df.join(df_topusers,on="user_id",how="inner").select("user_id","transaction_amount","transaction_date")

wind_spec = Window.partitionBy("user_id").orderBy(col("transaction_date").desc())

final_result = df_topthree.withColumn("rank",rank().over(wind_spec)).filter(col("rank")==1).drop("rank")

final_result.show(truncate=False)


