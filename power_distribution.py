from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("power").getOrCreate()

data = [
  ("C001", "2024-01-01"),
  ("C001", "2024-01-02"),
  ("C001", "2024-01-04"),
  ("C001", "2024-01-06"),
  ("C002", "2024-01-03"),
  ("C002", "2024-01-05"),
]

df = spark.createDataFrame(data,["customer_id", "billing_date"])

window_spec = Window.partitionBy("customer_id").orderBy("billing_date")

df_leaddate = df.withColumn("lead_date",lead("billing_date",1).over(window_spec))

date_diff =  df_leaddate.withColumn("date_diff",date_diff("lead_date","billing_date")).filter(col("date_diff")>1)

final_result = date_diff.withColumn("missing_from",date_add(col("billing_date"),1)).withColumn("missing_to",date_sub(col("lead_date"),1)).drop("billing_date","lead_date","date_diff")


final_result.show(truncate=False)







