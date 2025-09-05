from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("salting").getOrCreate()
data = [("Store A", "2024-01", 800),
          ("Store A", "2024-02", 1200),
          ("Store A", "2024-03", 900),
          ("Store B", "2024-01", 1500),
          ("Store B", "2024-02", 1600),
          ("Store B", "2024-03", 1400),
          ("Store C", "2024-01", 700),
          ("Store C", "2024-02", 1000),
          ("Store C", "2024-03", 800) ]

df_larger = spark.createDataFrame(data,["Store", "Month", "Sales"])

data = [(1,"laptop","2024-08-01"),
        (1,"Mouse","2024-08-05"),
        (2,"Keyboard","2024-08-02"),
        (2,"Monitor","2024-08-03")]

df_smaller = spark.createDataFrame(data,["customer_id","product","purchase_date"])

salted_large = df_larger.withColumn("salt",rand()*10).withColumn("salted_key",concat_ws("_",col("Store"),col("salt")))

salts = spark.range(0,10).toDF("salt")

lower_join = df_smaller.crossJoin(salts)

salted_smaller = lower_join.withColumn("salted_key",concat_ws("_",col("customer_id"),col("salt"))).show()

final_result =  salted_large.join(salted_smaller,"salted_key").show()




