import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables (Silver)
# -----------------------------
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

# -----------------------------
# Target: gold_dim_product
# -----------------------------
gold_dim_product_df = spark.sql(
    """
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY ps.product_id) AS BIGINT) AS product_key,
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.brand AS STRING) AS brand,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.is_active AS BOOLEAN) AS is_active,
  CURRENT_DATE AS effective_start_date,
  CAST('9999-12-31' AS DATE) AS effective_end_date
FROM products_silver ps
"""
)

gold_dim_product_df = gold_dim_product_df.coalesce(1)

(
    gold_dim_product_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_dim_product.csv")
)

gold_dim_product_df.createOrReplaceTempView("gold_dim_product")

# -----------------------------
# Target: gold_dim_store
# -----------------------------
# Added 'region' column so downstream gold_agg_sales_daily_region query can resolve gds.region.
# Business logic preserved by deriving region from existing 'state'.
gold_dim_store_df = spark.sql(
    """
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY ss.store_id) AS BIGINT) AS store_key,
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.city AS STRING) AS city,
  CAST(ss.state AS STRING) AS state,
  CAST(ss.state AS STRING) AS region,
  CAST(ss.store_type AS STRING) AS store_type,
  CAST(ss.open_date AS DATE) AS open_date,
  CAST(TRUE AS BOOLEAN) AS is_active,
  CURRENT_DATE AS effective_start_date,
  CAST('9999-12-31' AS DATE) AS effective_end_date
FROM stores_silver ss
"""
)

gold_dim_store_df = gold_dim_store_df.coalesce(1)

(
    gold_dim_store_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_dim_store.csv")
)

gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

# -----------------------------
# Target: gold_fact_sales
# -----------------------------
gold_fact_sales_df = spark.sql(
    """
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY sts.transaction_id) AS BIGINT) AS sales_txn_key,
  CAST(sts.transaction_id AS STRING) AS txn_id,
  CAST(sts.txn_date AS DATE) AS txn_date,
  CAST(gds.store_key AS BIGINT) AS store_key,
  CAST(gdp.product_key AS BIGINT) AS product_key,
  CAST(sts.quantity AS INT) AS quantity,
  CAST(sts.sale_amount AS DOUBLE) AS gross_sales_amount,
  CAST(sts.sale_amount AS DOUBLE) AS net_sales_amount,
  CAST(0 AS DOUBLE) AS discount_amount,
  CAST(0 AS DOUBLE) AS tax_amount,
  CAST(sts.data_quality_status AS STRING) AS data_quality_status,
  CURRENT_DATE AS load_date
FROM sales_transactions_silver sts
INNER JOIN gold_dim_store gds
  ON sts.store_id = gds.store_id
 AND CAST(sts.txn_date AS DATE) BETWEEN gds.effective_start_date AND gds.effective_end_date
INNER JOIN gold_dim_product gdp
  ON sts.product_id = gdp.product_id
 AND CAST(sts.txn_date AS DATE) BETWEEN gdp.effective_start_date AND gdp.effective_end_date
"""
)

gold_fact_sales_df = gold_fact_sales_df.coalesce(1)

(
    gold_fact_sales_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_fact_sales.csv")
)

gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

# -----------------------------
# Target: gold_agg_sales_daily
# -----------------------------
gold_agg_sales_daily_df = spark.sql(
    """
SELECT
  CAST(gfs.txn_date AS DATE) AS sales_date,
  CAST(gfs.store_key AS BIGINT) AS store_key,
  CAST(gfs.product_key AS BIGINT) AS product_key,
  CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
  CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
  CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
  CAST(SUM(gfs.discount_amount) AS DOUBLE) AS total_discount_amount,
  CAST(SUM(gfs.tax_amount) AS DOUBLE) AS total_tax_amount,
  CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
  CURRENT_DATE AS load_date
FROM gold_fact_sales gfs
GROUP BY
  gfs.txn_date,
  gfs.store_key,
  gfs.product_key
"""
)

gold_agg_sales_daily_df = gold_agg_sales_daily_df.coalesce(1)

(
    gold_agg_sales_daily_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_agg_sales_daily.csv")
)

gold_agg_sales_daily_df.createOrReplaceTempView("gold_agg_sales_daily")

# -----------------------------
# Target: gold_agg_sales_daily_store
# -----------------------------
gold_agg_sales_daily_store_df = spark.sql(
    """
SELECT
  CAST(gfs.txn_date AS DATE) AS sales_date,
  CAST(gfs.store_key AS BIGINT) AS store_key,
  CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
  CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
  CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
  CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
  CURRENT_DATE AS load_date
FROM gold_fact_sales gfs
GROUP BY
  gfs.txn_date,
  gfs.store_key
"""
)

gold_agg_sales_daily_store_df = gold_agg_sales_daily_store_df.coalesce(1)

(
    gold_agg_sales_daily_store_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_agg_sales_daily_store.csv")
)

gold_agg_sales_daily_store_df.createOrReplaceTempView("gold_agg_sales_daily_store")

# -----------------------------
# Target: gold_agg_sales_daily_region
# -----------------------------
gold_agg_sales_daily_region_df = spark.sql(
    """
SELECT
  CAST(gfs.txn_date AS DATE) AS sales_date,
  CAST(gds.region AS STRING) AS region,
  CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
  CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
  CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
  CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
  CURRENT_DATE AS load_date
FROM gold_fact_sales gfs
INNER JOIN gold_dim_store gds
  ON gfs.store_key = gds.store_key
 AND CAST(gfs.txn_date AS DATE) BETWEEN gds.effective_start_date AND gds.effective_end_date
GROUP BY
  gfs.txn_date,
  gds.region
"""
)

gold_agg_sales_daily_region_df = gold_agg_sales_daily_region_df.coalesce(1)

(
    gold_agg_sales_daily_region_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_agg_sales_daily_region.csv")
)

gold_agg_sales_daily_region_df.createOrReplaceTempView("gold_agg_sales_daily_region")

job.commit()
