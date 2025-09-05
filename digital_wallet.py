from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("digital_wallet").getOrCreate()

data = [  ("C001", 500, "2024-01-05"),
  ("C001", 200, "2024-01-01"),
  ("C002", 150, "2024-01-03"),
  ("C001", 700, "2024-01-10"),
  ("C002", 300, "2024-01-02")]

columns = ["customer_id", "txn_amount", "txn_date"]

df = spark.createDataFrame(data,columns)


wspec_first = Window.partitionBy("customer_id").orderBy("customer_id","txn_date")

df_first = df.withColumn("rnum",row_number().over(wspec_first)).filter(col("rnum")==1).drop("rnum")

wsec_last = Window.partitionBy("customer_id").orderBy(col("customer_id"),col("txn_date").desc())

df_last =  df.withColumn("rnum",row_number().over(wsec_last)).filter(col("rnum")==1).drop("rnum")

df_final_result = df_first.unionAll(df_last)

df_final_result.orderBy(col("customer_id"),col("txn_date").asc()).show(truncate=False)



