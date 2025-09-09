from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("Customer Transaction Pipeline").getOrCreate()
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


# Paths
customer_csv_path = "/path/to/customer.csv"
transactions_csv_path = "/path/to/transactions.csv"
output_delta_path_base = "/path/to/delta_output"

# Read CSV files
df_customers = spark.read.option("header", True).csv(customer_csv_path)
df_transactions = spark.read.option("header", True).csv(transactions_csv_path)

# Cast columns
df_transactions = df_transactions \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Join Data
df_joined = df_transactions.join(df_customers, on="customer_id", how="inner")

# Create month column
df_joined = df_joined.withColumn("month", date_format("transaction_date", "yyyy-MM"))

# ----------------------------------------------------
# 1️⃣ Revenue per day (grouped by transaction_date)
# ----------------------------------------------------
df_revenue_per_day = df_joined.groupBy("transaction_date").agg(
     sum("amount").alias("daily_revenue")
)

# ------------------------------------------------------------
# 2️⃣ Transactions per customer per day
# ------------------------------------------------------------
df_txn_per_customer_day = df_joined.groupBy("customer_id", "transaction_date").agg(
    count("transaction_id").alias("transactions_count")
)

# ------------------------------------------------------------
# 3️⃣ Revenue per month per region
# ------------------------------------------------------------
df_month_region = df_joined.groupBy("month", "region").agg(
    sum("amount").alias("monthly_revenue")
)

# -------------------------
# 📝 Write to Delta Tables
# -------------------------
df_revenue_per_day.write.format("delta").mode("overwrite").save(f"{output_delta_path_base}/revenue_per_day")
df_txn_per_customer_day.write.format("delta").mode("overwrite").save(f"{output_delta_path_base}/transactions_per_customer_day")
df_month_region.write.format("delta").mode("overwrite").save(f"{output_delta_path_base}/monthly_region_revenue")
