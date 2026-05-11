from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col

# Initialize Spark and Glue Contexts
spark = SparkSession.builder.appName("AWS Glue PySpark").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source data for sales transactions
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

# Create temp view for sales transactions
sales_transactions_df.createOrReplaceTempView("sts")

# Transform and write the gold_sales_transactions table
gold_sales_transactions_df = spark.sql("""
    SELECT 
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue
    FROM sts
""")

# Write to target path as a single CSV
gold_sales_transactions_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_sales_transactions.csv")

# Read source data for product master
product_master_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")

# Create temp view for product master
product_master_df.createOrReplaceTempView("pms")

# Transform and write the gold_product_master table
gold_product_master_df = spark.sql("""
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        TRIM(pms.product_name) AS product_name,
        TRIM(pms.product_category) AS product_category,
        CAST(pms.list_price AS DOUBLE) AS list_price
    FROM pms
""")

# Write to target path as a single CSV
gold_product_master_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_product_master.csv")

# Read source data for store master
store_master_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")

# Create temp view for store master
store_master_df.createOrReplaceTempView("sms")

# Transform and write the gold_store_master table
gold_store_master_df = spark.sql("""
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        TRIM(sms.store_name) AS store_name,
        TRIM(sms.region) AS region,
        TRIM(sms.store_manager) AS store_manager
    FROM sms
""")

# Write to target path as a single CSV
gold_store_master_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_store_master.csv")

spark.stop()