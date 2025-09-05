from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType

# Step 1: Create Spark session
spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()

# Step 2: Define schema and input data
data = [
    (1, "Laptop"),
    (2, "Tablet"),
    (3, "Smartphone"),
    (4, "Monitor"),
    (5, "Keyboard")
]

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Step 3: Add SCD Type-2 columns
scd_df = df.withColumn("start_date", current_date()) \
           .withColumn("end_date", lit("9999-12-31").cast(DateType())) \
           .withColumn("current_flag", lit(True).cast(BooleanType()))

# Show final SCD Type-2 DataFrame
scd_df.show(truncate=False)