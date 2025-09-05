from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("purchase").getOrCreate()

data = [(1,"laptop","2024-08-01"),
        (1,"Mouse","2024-08-05"),
        (2,"Keyboard","2024-08-02"),
        (2,"Monitor","2024-08-03")]

df = spark.createDataFrame(data,["customer_id","product","purchase_date"])

window_spec = Window.partitionBy("customer_id").orderBy(col("purchase_date").desc())

duplicated_df = df.withColumn("row_number",row_number().over(window_spec)).filter(col("row_number")==1).drop("row_number")

duplicated_df.show()