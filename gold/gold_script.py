import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =========================
# Read Source Tables (S3)
# =========================

stcs_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_clean_silver.{FILE_FORMAT}/")
)
stcs_df.createOrReplaceTempView("sales_transactions_clean_silver")

sfs_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_fact_silver.{FILE_FORMAT}/")
)
sfs_df.createOrReplaceTempView("sales_fact_silver")

ps_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("product_silver")

ss_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("store_silver")

dsas_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_agg_silver.{FILE_FORMAT}/")
)
dsas_df.createOrReplaceTempView("daily_sales_agg_silver")

# =========================
# Target: gold_cleaned_sales
# =========================

gold_cleaned_sales_df = spark.sql(
    """
SELECT
  CAST(stcs.clean_transaction_id AS STRING) AS clean_transaction_id,
  CAST(stcs.clean_product_id AS STRING) AS clean_product_id,
  CAST(stcs.clean_store_id AS STRING) AS clean_store_id,
  CAST(stcs.clean_quantity_sold AS INT) AS clean_quantity_sold,
  CAST(stcs.clean_sales_amount AS DOUBLE) AS clean_sales_amount,
  CAST(stcs.clean_transaction_date AS DATE) AS clean_transaction_date,
  CAST(stcs.clean_customer_id AS STRING) AS clean_customer_id
FROM sales_transactions_clean_silver stcs
"""
)

(
    gold_cleaned_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_sales.csv")
)

# =========================
# Target: gold_sales
# =========================

gold_sales_df = spark.sql(
    """
SELECT
  CAST(sfs.transaction_id AS STRING) AS transaction_id,
  CAST(sfs.product_id AS STRING) AS product_id,
  CAST(sfs.store_id AS STRING) AS store_id,
  CAST(sfs.quantity_sold AS INT) AS quantity_sold,
  CAST(sfs.sales_amount AS DOUBLE) AS sales_amount,
  CAST(sfs.transaction_date AS DATE) AS transaction_date,
  CAST(sfs.customer_id AS STRING) AS customer_id
FROM sales_fact_silver sfs
"""
)

(
    gold_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# =========================
# Target: gold_product
# =========================

gold_product_df = spark.sql(
    """
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.brand AS STRING) AS brand,
  CAST(ps.price AS FLOAT) AS price
FROM product_silver ps
"""
)

(
    gold_product_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# =========================
# Target: gold_store
# =========================

gold_store_df = spark.sql(
    """
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location,
  CAST(ss.store_type AS STRING) AS store_type
FROM store_silver ss
"""
)

(
    gold_store_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# =========================
# Target: gold_aggregated_sales
# =========================

gold_aggregated_sales_df = spark.sql(
    """
SELECT
  CAST(dsas.date AS DATE) AS date,
  CAST(dsas.store_id AS STRING) AS store_id,
  CAST(dsas.product_id AS STRING) AS product_id,
  CAST(dsas.total_quantity_sold AS INT) AS total_quantity_sold,
  CAST(dsas.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM daily_sales_agg_silver dsas
"""
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()
