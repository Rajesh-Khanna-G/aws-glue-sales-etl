# AWS Glue Sales ETL Pipeline

## Overview
This project demonstrates an end-to-end AWS Glue ETL pipeline that processes raw CSV sales data stored in Amazon S3, transforms it using PySpark, and makes it queryable using Amazon Athena.

## Architecture
S3 (raw data)
→ AWS Glue Crawler
→ AWS Glue Data Catalog
→ AWS Glue PySpark ETL Job
→ S3 (processed Parquet, partitioned)
→ AWS Glue Crawler
→ Amazon Athena

## Technologies Used
- AWS Glue (Crawler, ETL Job)
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- PySpark

## Data Transformations
- Converted order_date to DATE
- Added derived column total_amount = quantity × price
- Filtered header row issues from raw CSV
- Stored output in Parquet format
- Partitioned data by region for performance

## How to Run
1. Upload CSV files to S3 `raw/sales/`
2. Run Glue crawler to create raw table
3. Execute Glue PySpark ETL job
4. Run processed data crawler
5. Query processed data using Amazon Athena

## Sample Athena Query
```sql
SELECT
  region,
  SUM(total_amount) AS total_revenue
FROM processed_sales
GROUP BY region
ORDER BY total_revenue DESC;
