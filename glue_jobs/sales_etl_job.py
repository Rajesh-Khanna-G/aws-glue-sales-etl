import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Catalog (Bookmark enabled automatically)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="sales_db",
    table_name="sales"
)

# Convert to Spark DataFrame
df = dynamic_frame.toDF()

# Transformations
transformed_df = (
    df
    .filter(col("region") != "region")
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount", col("quantity") * col("price"))
)
    
# Write to S3 in append mode
(
    transformed_df.write
    .mode("append")
    .partitionBy("region")
    .parquet("s3://glue-sales-etl-rajesh/processed/sales/")
)

# Commit bookmark state
job.commit()
