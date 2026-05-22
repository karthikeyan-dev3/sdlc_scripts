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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# Read Source Tables (S3)
# =========================
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

gold_refresh_log_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/gold_refresh_log.{FILE_FORMAT}/")
)
gold_refresh_log_df.createOrReplaceTempView("gold_refresh_log")

# =========================
# Target: products_silver
# =========================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    CAST(pb.price AS FLOAT) AS price,
    CAST(pb.is_active AS BOOLEAN) AS is_active
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY is_active DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM dedup
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =========================
# Target: stores_silver
# =========================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(TRIM(sb.state) AS STRING) AS state,
    CAST(TRIM(sb.store_type) AS STRING) AS store_type,
    CAST(sb.open_date AS DATE) AS open_date
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    open_date,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM dedup
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =========================
# Target: sales_transactions_silver
# =========================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS revenue,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(DATE(stb.transaction_time) AS DATE) AS transaction_date
  FROM sales_transactions_bronze stb
),
filtered AS (
  SELECT
    b.transaction_id,
    b.product_id,
    b.store_id,
    b.quantity_sold,
    b.revenue,
    b.transaction_time,
    b.transaction_date
  FROM base b
  WHERE b.transaction_id IS NOT NULL
    AND b.product_id IS NOT NULL
    AND b.store_id IS NOT NULL
    AND b.quantity_sold IS NOT NULL
    AND b.quantity_sold > 0
),
joined AS (
  SELECT
    f.transaction_id,
    f.product_id,
    f.store_id,
    f.quantity_sold,
    f.revenue,
    f.transaction_time,
    f.transaction_date
  FROM filtered f
  INNER JOIN products_silver ps
    ON f.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON f.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    quantity_sold,
    revenue,
    transaction_date,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM joined
)
SELECT
  transaction_id,
  product_id,
  store_id,
  quantity_sold,
  revenue,
  transaction_date
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =========================
# Target: sales_summary_silver
# =========================
sales_summary_silver_sql = """
SELECT
  CAST(sts.transaction_date AS DATE) AS report_date,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT) AS total_sales,
  CAST(SUM(sts.revenue) AS DOUBLE) AS total_revenue,
  CAST(SUM(sts.quantity_sold) AS INT) AS total_units_sold,
  CAST(COUNT(DISTINCT sts.store_id) AS INT) AS store_count,
  CAST(COUNT(DISTINCT sts.product_id) AS INT) AS product_count
FROM sales_transactions_silver sts
GROUP BY CAST(sts.transaction_date AS DATE)
"""
sales_summary_silver_df = spark.sql(sales_summary_silver_sql)
sales_summary_silver_df.createOrReplaceTempView("sales_summary_silver")

(
    sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_summary_silver.csv")
)

# =========================
# Target: refresh_log_silver
# =========================
refresh_log_silver_sql = """
SELECT
  grl.refresh_date AS refresh_date,
  grl.status AS status,
  grl.records_processed AS records_processed,
  grl.success AS success,
  grl.failure_reason AS failure_reason
FROM gold_refresh_log grl
"""
refresh_log_silver_df = spark.sql(refresh_log_silver_sql)
refresh_log_silver_df.createOrReplaceTempView("refresh_log_silver")

(
    refresh_log_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/refresh_log_silver.csv")
)

job.commit()