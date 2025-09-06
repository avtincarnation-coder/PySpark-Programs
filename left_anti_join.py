from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("left_anti").getOrCreate()

data_emp = [(1,100),(2,200),(3,300),(4,400)]

empDf = spark.createDataFrame(data_emp,["empid","deptid"])

data_dept = [("finanace",100),("hr",400)]

deptDf = spark.createDataFrame(data_dept,["deptname","deptid"])

leftanti_result = empDf.join(deptDf,on = "deptid", how="left_anti")

leftanti_result.show(truncate=False)

