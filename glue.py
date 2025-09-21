import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ✅ Glue job arguments (job name etc.)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ✅ Spark & Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Read data from S3 (CSV)
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://my-input-bucket/employees/"], "recurse": True}
)

# ✅ Convert DynamicFrame to Spark DataFrame for transformations
df = datasource.toDF()

# ✅ Transformation: select and filter
filtered_df = df.select("empid", "empname", "salary") \
                .filter(df["salary"] > 50000)

# ✅ Convert back to DynamicFrame
final_dyf = DynamicFrame.fromDF(filtered_df, glueContext, "final_dyf")

# ✅ Write transformed data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://my-output-bucket/employees_parquet/", "partitionKeys": []}
)

job.commit()
