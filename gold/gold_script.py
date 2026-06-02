import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# NOTE: getResolvedOptions requires at least one option name in AWS Glue.
# It was unused in the original code and can cause runtime errors, so it was removed
# to preserve the intended business logic.

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------------------------------------------------------------
# Read Source Tables (Silver) and Create Temp Views
# -----------------------------------------------------------------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sts")

ssds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_store_daily_silver.{FILE_FORMAT}/")
)
ssds_df.createOrReplaceTempView("ssds")

spds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_product_daily_silver.{FILE_FORMAT}/")
)
spds_df.createOrReplaceTempView("spds")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("pms")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("sms")

dqs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)
dqs_df.createOrReplaceTempView("dqs")

# -----------------------------------------------------------------------------------
# Target: gold_sales_transactions
# -----------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql(
    """
SELECT
  CAST(sts.transaction_id AS STRING)       AS transaction_id,
  CAST(sts.transaction_date AS DATE)       AS transaction_date,
  CAST(sts.store_id AS STRING)             AS store_id,
  CAST(sts.product_id AS STRING)           AS product_id,
  CAST(sts.quantity_sold AS INT)           AS quantity_sold,
  CAST(sts.revenue AS DECIMAL(18,2))       AS revenue
FROM sts
"""
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_sales_aggregated_store
# -----------------------------------------------------------------------------------

gold_sales_aggregated_store_df = spark.sql(
    """
SELECT
  CAST(ssds.store_id AS STRING)              AS store_id,
  CAST(ssds.date AS DATE)                    AS date,
  CAST(ssds.total_revenue AS DECIMAL(18,2))  AS total_revenue,
  CAST(ssds.total_transactions AS BIGINT)    AS total_transactions,
  CAST(ssds.total_quantity_sold AS BIGINT)   AS total_quantity_sold
FROM ssds
"""
)

(
    gold_sales_aggregated_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_store.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_sales_aggregated_product
# -----------------------------------------------------------------------------------

gold_sales_aggregated_product_df = spark.sql(
    """
SELECT
  CAST(spds.product_id AS STRING)            AS product_id,
  CAST(spds.date AS DATE)                    AS date,
  CAST(spds.total_revenue AS DECIMAL(18,2))  AS total_revenue,
  CAST(spds.total_sold AS BIGINT)            AS total_sold,
  CAST(spds.category_id AS STRING)           AS category_id
FROM spds
"""
)

(
    gold_sales_aggregated_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_product.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_product_master
# -----------------------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
SELECT
  CAST(pms.product_id AS STRING)      AS product_id,
  CAST(pms.product_name AS STRING)    AS product_name,
  CAST(pms.category_id AS STRING)     AS category_id,
  CAST(pms.category_name AS STRING)   AS category_name,
  CAST(pms.brand AS STRING)           AS brand
FROM pms
"""
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_store_master
# -----------------------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
SELECT
  CAST(sms.store_id AS STRING)      AS store_id,
  CAST(sms.store_name AS STRING)    AS store_name,
  CAST(sms.location AS STRING)      AS location,
  CAST(sms.region AS STRING)        AS region
FROM sms
"""
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_data_quality
# -----------------------------------------------------------------------------------

gold_data_quality_df = spark.sql(
    """
SELECT
  CAST(dqs.record_id AS STRING)          AS record_id,
  CAST(dqs.source_table AS STRING)       AS source_table,
  CAST(dqs.is_valid AS BOOLEAN)          AS is_valid,
  CAST(dqs.error_description AS STRING)  AS error_description
FROM dqs
"""
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)
