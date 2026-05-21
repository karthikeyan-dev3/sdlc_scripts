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

# ----------------------------
# Read source tables from S3
# ----------------------------
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

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: gold.products
# Mapping: silver.products_silver ps
# ============================================================
products_sql = """
SELECT
  CAST(ps.product_id AS STRING)   AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.brand AS STRING)        AS brand,
  CAST(ps.price AS FLOAT)         AS price,
  CAST(ps.is_active AS BOOLEAN)   AS is_active
FROM products_silver ps
"""
products_out_df = spark.sql(products_sql)

(
    products_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products.csv")
)

# ============================================================
# Target: gold.stores
# Mapping: silver.stores_silver ss
# ============================================================
stores_sql = """
SELECT
  CAST(ss.store_id AS STRING)    AS store_id,
  CAST(ss.store_name AS STRING)  AS store_name,
  CAST(ss.city AS STRING)        AS city,
  CAST(ss.state AS STRING)       AS state,
  CAST(ss.store_type AS STRING)  AS store_type,
  CAST(ss.open_date AS DATE)     AS open_date
FROM stores_silver ss
"""
stores_out_df = spark.sql(stores_sql)

(
    stores_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores.csv")
)

# ============================================================
# Target: gold.dates
# Mapping: silver.sales_transactions_silver sts
# ============================================================
dates_sql = """
SELECT DISTINCT
  DATE(sts.transaction_time) AS transaction_date
FROM sales_transactions_silver sts
"""
dates_out_df = spark.sql(dates_sql)

(
    dates_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/dates.csv")
)

# ============================================================
# Target: gold.sales_transactions
# Mapping:
# silver.sales_transactions_silver sts
# INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ============================================================
sales_transactions_sql = """
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.store_id AS STRING)       AS store_id,
  CAST(sts.product_id AS STRING)     AS product_id,
  CAST(sts.quantity AS INT)          AS quantity,
  CAST(sts.sale_amount AS DOUBLE)    AS sale_amount,
  CAST(sts.transaction_time AS TIMESTAMP) AS transaction_time
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
"""
sales_transactions_out_df = spark.sql(sales_transactions_sql)

(
    sales_transactions_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions.csv")
)

# ============================================================
# Target: gold.daily_sales_store
# Mapping:
# silver.sales_transactions_silver sts
# INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ============================================================
daily_sales_store_sql = """
SELECT
  DATE(sts.transaction_time)      AS transaction_date,
  CAST(sts.store_id AS STRING)    AS store_id,
  SUM(CAST(sts.quantity AS INT))  AS total_quantity,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_sales_amount
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  DATE(sts.transaction_time),
  CAST(sts.store_id AS STRING)
"""
daily_sales_store_out_df = spark.sql(daily_sales_store_sql)

(
    daily_sales_store_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_store.csv")
)

# ============================================================
# Target: gold.daily_sales_product
# Mapping:
# silver.sales_transactions_silver sts
# INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ============================================================
daily_sales_product_sql = """
SELECT
  DATE(sts.transaction_time)      AS transaction_date,
  CAST(sts.product_id AS STRING)  AS product_id,
  SUM(CAST(sts.quantity AS INT))  AS total_quantity,
  SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_sales_amount
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  DATE(sts.transaction_time),
  CAST(sts.product_id AS STRING)
"""
daily_sales_product_out_df = spark.sql(daily_sales_product_sql)

(
    daily_sales_product_out_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_product.csv")
)

job.commit()