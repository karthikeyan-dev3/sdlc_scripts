import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source table: silver_sales_summary
silver_sales_summary_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/silver_sales_summary.{FILE_FORMAT}/")

silver_sales_summary_df.createOrReplaceTempView("silver_sales_summary")

# Transform and write gold_sales_performance
gold_sales_performance_df = spark.sql("""
    SELECT 
        sss.transaction_id,
        sss.store_id,
        sss.product_id,
        CAST(sss.transaction_date AS DATE) AS transaction_date,
        sss.total_revenue,
        sss.quantity_sold,
        sss.store_location,
        sss.product_category
    FROM silver_sales_summary sss
""")

gold_sales_performance_df.coalesce(1).write \
    .format(FILE_FORMAT) \
    .mode("overwrite") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")

# Read source table: store_master_silver
store_master_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# Transform and write gold_store_master
gold_store_master_df = spark.sql("""
    SELECT 
        sms.store_id,
        sms.store_name,
        sms.store_region
    FROM store_master_silver sms
""")

gold_store_master_df.coalesce(1).write \
    .format(FILE_FORMAT) \
    .mode("overwrite") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_store_master.csv")

# Read source table: product_master_silver
product_master_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# Transform and write gold_product_master
gold_product_master_df = spark.sql("""
    SELECT 
        pms.product_id,
        pms.product_name,
        pms.product_category
    FROM product_master_silver pms
""")

gold_product_master_df.coalesce(1).write \
    .format(FILE_FORMAT) \
    .mode("overwrite") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_product_master.csv")