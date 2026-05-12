from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths and format
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Read source table for gold_sales_performance
silver_sales_enrichment_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/silver_sales_enrichment.{FILE_FORMAT}/")

# Create temporary view for sales_enrichment
silver_sales_enrichment_df.createOrReplaceTempView("sse")

# Transformations for gold_sales_performance
gold_sales_performance_sql = """
SELECT 
    sse.transaction_id,
    sse.transaction_date,
    sse.product_id,
    sse.store_id,
    sse.sales_amount,
    sse.quantity_sold,
    sse.product_category,
    sse.store_region
FROM sse
"""

gold_sales_performance_df = spark.sql(gold_sales_performance_sql)

# Write gold_sales_performance
gold_sales_performance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_sales_performance.csv")

# Read source table for gold_product_master
silver_product_details_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/silver_product_details.{FILE_FORMAT}/")

# Create temporary view for product_details
silver_product_details_df.createOrReplaceTempView("spd")

# Transformations for gold_product_master
gold_product_master_sql = """
SELECT 
    spd.product_id,
    spd.product_name,
    spd.product_category,
    spd.product_price
FROM spd
"""

gold_product_master_df = spark.sql(gold_product_master_sql)

# Write gold_product_master
gold_product_master_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_product_master.csv")

# Read source table for gold_store_master
silver_store_details_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/silver_store_details.{FILE_FORMAT}/")

# Create temporary view for store_details
silver_store_details_df.createOrReplaceTempView("ssd")

# Transformations for gold_store_master
gold_store_master_sql = """
SELECT 
    ssd.store_id,
    ssd.store_location,
    ssd.store_region,
    ssd.store_manager
FROM ssd
"""

gold_store_master_df = spark.sql(gold_store_master_sql)

# Write gold_store_master
gold_store_master_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_store_master.csv")