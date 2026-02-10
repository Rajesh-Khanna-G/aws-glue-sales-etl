from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.getOrCreate()

# Read raw data from Glue Catalog
raw_df = spark.sql("""
    SELECT *
    FROM sales_db.sales
""")

# Transformations
transformed_df = (
    raw_df
    .filter(col("region") != "region")
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount", col("quantity") * col("price"))
)

# Write processed data to S3
transformed_df.write \
    .mode("overwrite") \
    .partitionBy("region")\
    .parquet("s3://glue-sales-etl-rajesh/processed/sales/")
