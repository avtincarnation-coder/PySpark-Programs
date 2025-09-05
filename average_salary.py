from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("average salary").getOrCreate()

data = [
("HR", "2020", 2500),
("HR", "2021", 3200),
("HR", "2022", 2800),
("Engineering", "2020", 5000),
("Engineering", "2021", 6000),
("Engineering", "2022", 5500),
("Marketing", "2020", 4000),
("Marketing", "2021", 3500),
("Marketing", "2022", 3300)
]

columns = ["Department", "Year", "Salary"]

df = spark.createDataFrame(data,columns)


df_avg = df.groupBy("Department").agg(avg(col("salary")).alias("avgsal"))

df_filter = df_avg.filter(col("avgsal")<3000)

df_total_sal = df.groupBy("Department").agg(sum("salary").alias("Total_Salary")).orderBy(col("Total_Salary").desc())

df_total_sal.show()














