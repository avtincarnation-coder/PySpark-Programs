from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("read_json").getOrCreate()

df = spark.read.format("json").option("multiline","true").load("D:/java/data_new/sample2.json")

df.printSchema()


exploded_df = df.withColumn("phoneNumbers",explode(col("phoneNumbers")))

df = exploded_df.select("address.city",
                        "address.state",
                        "address.streetAddress",
                        "age",
                        "firstName",
                        "gender",
                        "lastName",
                        "phoneNumbers.number",
                        "phoneNumbers.type")


df.show()

