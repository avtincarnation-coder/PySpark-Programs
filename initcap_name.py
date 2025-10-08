from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType


spark  = SparkSession.builder.appName("delta tables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .getOrCreate()

schema = StructType([StructField('fruit1',StringType(),True),
                    StructField('fruit2',StringType(),True),
                    StructField('fruit3',StringType(),True)])

df = spark.read.csv("D:/java/sample.txt" , inferSchema=True, schema=schema,header=True)

df.show()

df.write.format("delta").mode("overwrite").save("D:/java/sample")

print("Delta table write successful")