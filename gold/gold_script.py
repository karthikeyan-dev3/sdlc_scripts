from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read the source table
retail_sales_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/retail_sales_enriched_silver.{FILE_FORMAT}/")
)

# Create a temporary view
retail_sales_df.createOrReplaceTempView("rses")

# SQL Query for Transformation
gold_retail_sales_query = """
SELECT
    transaction_id,
    transaction_date,
    product_id,
    product_name,
    product_category,
    store_id,
    store_location,
    store_type,
    quantity_sold,
    sales_amount,
    aggregation_date
FROM rses
"""

# Execute SQL query and store in DataFrame
gold_retail_sales_df = spark.sql(gold_retail_sales_query)

# Write the transformed DataFrame to target path as a single CSV file
gold_retail_sales_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_retail_sales.csv", header=True, mode='overwrite')