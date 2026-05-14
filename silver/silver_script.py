import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================
# 1) Read Source Tables
# =========================
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# =========================
# 2) Create Temp Views
# =========================
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =========================
# Target: silver.products_silver
# =========================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)  AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING)     AS category
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category
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
# Target: silver.stores_silver
# =========================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING)   AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name
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
# Target: silver.sales_transactions_silver
# =========================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(stb.product_id) AS STRING)     AS product_id,
    CAST(TRIM(stb.store_id) AS STRING)       AS store_id,
    CAST(stb.transaction_time AS TIMESTAMP)  AS transaction_time,
    CAST(stb.quantity AS INT)                AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE)          AS total_revenue
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND TRIM(stb.product_id) IS NOT NULL
    AND TRIM(stb.product_id) <> ''
    AND TRIM(stb.store_id) IS NOT NULL
    AND TRIM(stb.store_id) <> ''
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
conformed AS (
  SELECT
    b.transaction_id,
    b.product_id,
    b.store_id,
    b.transaction_time,
    b.quantity_sold,
    b.total_revenue
  FROM base b
  INNER JOIN products_silver ps
    ON b.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON b.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    CAST(transaction_time AS DATE) AS sale_date,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM conformed
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()