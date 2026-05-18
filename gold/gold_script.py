import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read source tables (S3) + create temp views
# ------------------------------------------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

aggregated_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_daily_silver.{FILE_FORMAT}/")
)
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

# ------------------------------------------------------------
# gold.gold_product_master
# Source: silver.products_silver ps
# ------------------------------------------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)   AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.brand AS STRING)        AS brand,
  CAST(ps.price AS FLOAT)         AS price
FROM products_silver ps
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------
# gold.gold_store_master
# Source: silver.stores_silver ss
# ------------------------------------------------------------
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)     AS store_id,
  CAST(ss.store_name AS STRING)   AS store_name,
  CAST(ss.location AS STRING)     AS location,
  CAST(ss.store_type AS STRING)   AS store_type
FROM stores_silver ss
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------
# gold.gold_sales_transactions
# Source: silver.sales_transactions_silver sts
# ------------------------------------------------------------
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING)     AS product_id,
  CAST(sts.store_id AS STRING)       AS store_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE)   AS sales_amount,
  CAST(sts.quantity_sold AS INT)     AS quantity_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------
# gold.gold_sales_cleaned
# Source: silver.sales_transactions_silver sts
# ------------------------------------------------------------
gold_sales_cleaned_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING)     AS product_id,
  CAST(sts.store_id AS STRING)       AS store_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE)   AS sales_amount,
  CAST(sts.quantity_sold AS INT)     AS quantity_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_cleaned_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_cleaned.csv")
)

# ------------------------------------------------------------
# gold.gold_aggregated_sales
# Source: silver.aggregated_sales_daily_silver asds
# ------------------------------------------------------------
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(asds.date AS DATE)               AS date,
  CAST(asds.total_sales AS DOUBLE)      AS total_sales,
  CAST(asds.total_quantity AS INT)      AS total_quantity,
  CAST(asds.average_price AS DOUBLE)    AS average_price,
  CAST(asds.total_transactions AS BIGINT) AS total_transactions
FROM aggregated_sales_daily_silver asds
""")

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()