from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window


spark = SparkSession.builder.appName("transaction").getOrCreate()

data = [(1,101,500.0,"2024-01-01"),
        (2,102,200.0,"2024-01-02"),
        (3,101,300.0,"2024-01-03"),
        (4,103,100.0,"2024-01-04"),
        (5,102,400.0,"2024-01-05"),
        (6,103,600.0,"2024-01-06"),
        (7,101,200.0,"2024-01-07")]

columns = ["transaction_id","user_id","transaction_amount","transaction_date"]

df = spark.createDataFrame(data,columns)

df = df.withColumn("transaction_date",to_date(col("transaction_date"),"yyyy-MM-dd"))

window_spec = Window.partitionBy("user_id").orderBy("transaction_date")

df = df.withColumn("prev_date",lag("transaction_date").over(window_spec))

df = df.withColumn("day_gap", datediff(col("transaction_date"),col("prev_date")))

avg_gap_df = df.groupBy("user_id").agg(avg("day_gap").alias("avg_day_gap"))

largest_avg_gap = avg_gap_df.orderBy(desc("avg_day_gap")).limit(1)


avg_gap_df.show()

largest_avg_gap.show()
