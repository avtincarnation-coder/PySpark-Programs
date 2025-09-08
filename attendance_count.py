from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("employee_attendance").getOrCreate()

data =  [(1, "Rahul", "IT", "2024-01-01", "Present"),
        (2, "Priya", "HR", "2024-01-01", "Absent"),
        (1, "Rahul", "IT", "2024-01-02", "Present"),
        (3, "Amit", "IT", "2024-01-01", "Present"),
        (2, "Priya", "HR", "2024-01-02", "Present"),
    (4, "Neha", "Finance", "2024-01-01", "Present")]

columns = ["emp_id", "emp_name", "dept", "log_date", "status"]


df = spark.createDataFrame(data,columns)

df_filtered = df.filter(col("status")=="Present")

df_present = df_filtered.groupBy("emp_id", "emp_name", "dept").agg(count(col("emp_id")).alias("total_present")).select("emp_id", "emp_name", "dept", "total_present")

window_spec = Window.partitionBy("dept").orderBy(col("total_present").desc())

final_result = df_present.withColumn("rank",rank().over(window_spec)).filter(col("rank")==1).drop("rank","emp_id")

final_result.select("dept","emp_name","total_present")

final_result.show(truncate=False)



