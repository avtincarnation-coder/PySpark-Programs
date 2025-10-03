from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("sales average").getOrCreate()

data = [(1,1001,123,'2022-03-01',10),
        (2,1002,265,'2022-03-04',5),
        (3,1001,478,'2022-03-15',7),
        (4,1003,192,'2022-04-01',3),
        (5,1002,123,'2022-05-04',8)]

columns = ["sale_id","product_id","customer_id","sale_date","quantity"]


df = spark.createDataFrame(data,columns)

wind_spec = Window.partitionBy(month("sale_date"),"product_id").orderBy(month("sale_date"))

final_df = df.withColumn("avg_sales",avg("quantity").over(wind_spec)).select(month("sale_date").alias("Sale Month"),"product_id","avg_sales")

final_result = final_df.distinct().orderBy(month("sale_date"))

final_result.show(truncate=False)




