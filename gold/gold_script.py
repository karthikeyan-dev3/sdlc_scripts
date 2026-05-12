import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Set source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Load source tables
sales_transactions_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

products_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")

stores_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

# Create temp views
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# Transformations for each table and save the results

# gold_sales_transactions
gold_sales_transactions_df = spark.sql("""
    SELECT 
        transaction_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        store_id,
        product_id,
        CAST(quantity_sold AS INT) AS quantity_sold,
        CAST(sales_amount AS DOUBLE) AS sales_amount
    FROM sales_transactions_silver
""")
gold_sales_transactions_df.write.csv(f"{TARGET_PATH}/gold_sales_transactions.csv", mode="overwrite", header=True)

# gold_product_master
gold_product_master_df = spark.sql("""
    SELECT 
        product_id,
        product_name,
        category,
        brand
    FROM products_silver
""")
gold_product_master_df.write.csv(f"{TARGET_PATH}/gold_product_master.csv", mode="overwrite", header=True)

# gold_store_master
gold_store_master_df = spark.sql("""
    SELECT 
        store_id,
        store_name,
        region,
        store_type
    FROM stores_silver
""")
gold_store_master_df.write.csv(f"{TARGET_PATH}/gold_store_master.csv", mode="overwrite", header=True)

# gold_integrated_sales
gold_integrated_sales_df = spark.sql("""
    SELECT 
        sts.transaction_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        sts.store_id,
        ss.store_name,
        ss.region,
        sts.product_id,
        ps.product_name,
        ps.category,
        ps.brand,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss ON sts.store_id = ss.store_id
    LEFT JOIN products_silver ps ON sts.product_id = ps.product_id
""")
gold_integrated_sales_df.write.csv(f"{TARGET_PATH}/gold_integrated_sales.csv", mode="overwrite", header=True)