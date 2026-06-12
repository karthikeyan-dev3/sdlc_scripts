import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
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

# -----------------------------
# Read Source Tables from S3
# -----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# Target: gold.gold_store_daily_sales (gold_store_daily_sales)
# ============================================================
gold_store_daily_sales_sql = """
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.store_id AS store_id,
  ss.store_name AS store_name,
  ss.store_type AS store_type,
  ss.city AS city,
  ss.state AS state,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count,
  SUM(CAST(sts.quantity AS BIGINT)) AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  CAST(sts.transaction_time AS DATE),
  sts.store_id,
  ss.store_name,
  ss.store_type,
  ss.city,
  ss.state
"""

gold_store_daily_sales_df = spark.sql(gold_store_daily_sales_sql)

(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# ============================================================
# Target: gold.gold_product_daily_sales (gold_product_daily_sales)
# ============================================================
gold_product_daily_sales_sql = """
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.product_id AS product_id,
  ps.product_name AS product_name,
  ps.brand AS brand,
  ps.category AS category,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count,
  SUM(CAST(sts.quantity AS BIGINT)) AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.transaction_time AS DATE),
  sts.product_id,
  ps.product_name,
  ps.brand,
  ps.category
"""

gold_product_daily_sales_df = spark.sql(gold_product_daily_sales_sql)

(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

# ============================================================
# Target: gold.gold_store_product_daily_sales (gold_store_product_daily_sales)
# ============================================================
gold_store_product_daily_sales_sql = """
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count,
  SUM(CAST(sts.quantity AS BIGINT)) AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.transaction_time AS DATE),
  sts.store_id,
  sts.product_id
"""

gold_store_product_daily_sales_df = spark.sql(gold_store_product_daily_sales_sql)

(
    gold_store_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_product_daily_sales.csv")
)

job.commit()