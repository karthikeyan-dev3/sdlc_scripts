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

# ---------------------------------------------------------------------------
# Read Source Tables (S3)
# ---------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
dqds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_silver.{FILE_FORMAT}/")
)

# ---------------------------------------------------------------------------
# Create Temp Views
# ---------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ss_df.createOrReplaceTempView("stores_silver")
ps_df.createOrReplaceTempView("products_silver")
dqds_df.createOrReplaceTempView("data_quality_daily_silver")

# ---------------------------------------------------------------------------
# Target: gold_sales_daily_store
# ---------------------------------------------------------------------------
gold_sales_daily_store_df = spark.sql(
    """
SELECT
  CAST(sts.sales_date AS DATE) AS sales_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.city AS STRING) AS store_city,
  CAST(ss.state AS STRING) AS store_state,
  CAST(ss.state AS STRING) AS store_region,
  CAST(ss.store_type AS STRING) AS store_type,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue_amount,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS total_transactions_count,
  CAST(SUM(CAST(sts.quantity AS BIGINT)) AS BIGINT) AS total_quantity_units,
  CAST(
    SUM(CAST(sts.sale_amount AS DOUBLE)) / NULLIF(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)), 0)
    AS DOUBLE
  ) AS avg_basket_value_amount,
  CAST(
    SUM(CAST(sts.quantity AS DOUBLE)) / NULLIF(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)), 0)
    AS DOUBLE
  ) AS avg_units_per_transaction,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON CAST(sts.store_id AS STRING) = CAST(ss.store_id AS STRING)
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING),
  CAST(ss.store_type AS STRING)
"""
)

(
    gold_sales_daily_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_sales_daily_product
# ---------------------------------------------------------------------------
gold_sales_daily_product_df = spark.sql(
    """
SELECT
  CAST(sts.sales_date AS DATE) AS sales_date,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.brand AS STRING) AS brand,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.category AS STRING) AS sub_category,
  CAST(ps.category AS STRING) AS department,
  CAST(ps.category AS STRING) AS unit_of_measure,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue_amount,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS total_transactions_count,
  CAST(SUM(CAST(sts.quantity AS BIGINT)) AS BIGINT) AS total_quantity_units,
  CAST(
    SUM(CAST(sts.sale_amount AS DOUBLE)) / NULLIF(SUM(CAST(sts.quantity AS DOUBLE)), 0)
    AS DOUBLE
  ) AS avg_selling_price_amount,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON CAST(sts.product_id AS STRING) = CAST(ps.product_id AS STRING)
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.brand AS STRING),
  CAST(ps.category AS STRING)
"""
)

(
    gold_sales_daily_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_product.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_sales_daily_store_product
# ---------------------------------------------------------------------------
gold_sales_daily_store_product_df = spark.sql(
    """
SELECT
  CAST(sts.sales_date AS DATE) AS sales_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.state AS STRING) AS store_region,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue_amount,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS total_transactions_count,
  CAST(SUM(CAST(sts.quantity AS BIGINT)) AS BIGINT) AS total_quantity_units,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON CAST(sts.store_id AS STRING) = CAST(ss.store_id AS STRING)
LEFT JOIN products_silver ps
  ON CAST(sts.product_id AS STRING) = CAST(ps.product_id AS STRING)
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.state AS STRING),
  CAST(sts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.category AS STRING)
"""
)

(
    gold_sales_daily_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store_product.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_data_quality_daily
# NOTE: Column-level UDT details for this target are not provided in the config.
# ---------------------------------------------------------------------------
gold_data_quality_daily_df = spark.sql(
    """
SELECT
  *
FROM data_quality_daily_silver dqds
"""
)

(
    gold_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_daily.csv")
)

job.commit()
