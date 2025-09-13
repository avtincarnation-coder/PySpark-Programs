from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("Employee Attendance").getOrCreate()

data =  [
(1, "2024-11-01"),
(1, "2024-11-02"),
(1, "2024-11-05"),
(2, "2024-11-03"),
(2, "2024-11-04"),
(2, "2024-11-05")
]

columns = ["employee_id", "attendance_date"]

df = spark.createDataFrame(data,columns)

window_spec = Window.partitionBy("employee_id").orderBy(col("attendance_date").asc())

next_date = df.withColumn("next_date",lag("attendance_date",1).over(window_spec))


date_diff = next_date.withColumn("date_diff",date_diff("attendance_date","next_date"))

date_diff.show()
