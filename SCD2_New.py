from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SCDType2").getOrCreate()

dim_customers = spark.read.format("delta").load("s3://retail/customer_dim")

incoming_customers = spark.read.option("header", True).csv("new_customers.csv")

changed = dim_customers.alias("d").join(incoming_customers.alias("i"), on ="customer_id" , how = "inner").filter(col("d.address") != col("i.address"))

expired = changed.withColumn("end_date",current_date()).withColumn("is_current",lit(False))

new_records = incoming_customers.withColumn("effective_date",current_date()).withColumn("end_date",lit(None)).withColumn("is_current",lit(False))

new_records.write.format("delta").mode("append").save("s3://retail/customer_dim")