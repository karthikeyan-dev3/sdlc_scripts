from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# Initialize GlueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# SALES_TRANSACTIONS_GOLD processing
sales_transactions_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
sales_transactions_silver_df.createOrReplaceTempView("sts")
sales_transactions_gold_df = spark.sql("""
    SELECT
        transaction_id,
        sales_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
    FROM sts
""")
sales_transactions_gold_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_transactions_gold.csv", header=True)

# PRODUCT_MASTER_GOLD processing
product_master_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
product_master_silver_df.createOrReplaceTempView("pms")
product_master_gold_df = spark.sql("""
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM pms
""")
product_master_gold_df.write.mode("overwrite").csv(TARGET_PATH + "/product_master_gold.csv", header=True)

# STORE_MASTER_GOLD processing
store_master_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
store_master_silver_df.createOrReplaceTempView("sms")
store_master_gold_df = spark.sql("""
    SELECT
        store_id,
        store_name,
        store_location,
        region
    FROM sms
""")
store_master_gold_df.write.mode("overwrite").csv(TARGET_PATH + "/store_master_gold.csv", header=True)

# SALES_AGGREGATE_GOLD processing
sales_aggregate_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_aggregate_silver.{FILE_FORMAT}/")
sales_aggregate_silver_df.createOrReplaceTempView("sas")
sales_aggregate_gold_df = spark.sql("""
    SELECT
        date,
        store_id,
        category,
        total_sales_amount,
        total_transactions,
        aggregated_level
    FROM sas
""")
sales_aggregate_gold_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_aggregate_gold.csv", header=True)
