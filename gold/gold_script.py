import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
# Read source tables from S3
# -----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: gold_sales_transaction_enriched
# ============================================================
gold_sales_transaction_enriched_df = spark.sql("""
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.transaction_id AS transaction_id,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  CAST(sts.quantity AS INT) AS quantity_sold,
  ps.category AS product_category,
  ps.brand AS product_brand,
  ss.store_name AS store_name,
  ss.city AS store_city
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
""")

(
    gold_sales_transaction_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transaction_enriched.csv")
)

# ============================================================
# Target: gold_store_daily_performance
# ============================================================
gold_store_daily_performance_df = spark.sql("""
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.store_id AS store_id,
  ss.store_name AS store_name,
  ss.city AS store_city,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count,
  SUM(CAST(sts.quantity AS BIGINT)) AS units_sold
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  CAST(sts.transaction_time AS DATE),
  sts.store_id,
  ss.store_name,
  ss.city
""")

(
    gold_store_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_performance.csv")
)

# ============================================================
# Target: gold_product_daily_performance
# ============================================================
gold_product_daily_performance_df = spark.sql("""
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  sts.product_id AS product_id,
  ps.product_name AS product_name,
  ps.category AS product_category,
  ps.brand AS product_brand,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  SUM(CAST(sts.quantity AS BIGINT)) AS units_sold,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.transaction_time AS DATE),
  sts.product_id,
  ps.product_name,
  ps.category,
  ps.brand
""")

(
    gold_product_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_performance.csv")
)

# ============================================================
# Target: gold_category_daily_performance
# ============================================================
gold_category_daily_performance_df = spark.sql("""
SELECT
  CAST(sts.transaction_time AS DATE) AS sales_date,
  ps.category AS product_category,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
  SUM(CAST(sts.quantity AS BIGINT)) AS units_sold,
  COUNT(DISTINCT sts.transaction_id) AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.transaction_time AS DATE),
  ps.category
""")

(
    gold_category_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_category_daily_performance.csv")
)

# ============================================================
# Target: gold_store_dimension
# ============================================================
gold_store_dimension_df = spark.sql("""
SELECT
  ss.store_id AS store_id,
  ss.store_name AS store_name,
  ss.store_type AS store_type,
  ss.open_date AS store_open_date,
  ss.city AS city,
  ss.state AS state_province
FROM stores_silver ss
""")

(
    gold_store_dimension_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_dimension.csv")
)

# ============================================================
# Target: gold_product_dimension
# ============================================================
gold_product_dimension_df = spark.sql("""
SELECT
  ps.product_id AS product_id,
  ps.product_name AS product_name,
  ps.brand AS brand,
  ps.category AS category
FROM products_silver ps
""")

(
    gold_product_dimension_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_dimension.csv")
)

job.commit()