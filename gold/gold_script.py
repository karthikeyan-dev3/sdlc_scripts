from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read source table from S3
sales_performance_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_performance_enriched_silver.{FILE_FORMAT}/")

# Create Temporary View
sales_performance_df.createOrReplaceTempView("spes")

# Spark SQL Transformation
gold_sales_performance_df = spark.sql("""
    SELECT 
        CAST(sales_transaction_id AS STRING) AS sales_transaction_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(store_id AS STRING) AS store_id,
        CAST(sales_amount AS DOUBLE) AS sales_amount,
        CAST(sales_date AS DATE) AS sales_date,
        TRIM(product_name) AS product_name,
        TRIM(product_category) AS product_category,
        TRIM(store_name) AS store_name,
        TRIM(store_region) AS store_region
    FROM spes
""")

# Write the result to the target S3 path
gold_sales_performance_df.coalesce(1).write \
    .option("header", "true") \
    .format(FILE_FORMAT) \
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")