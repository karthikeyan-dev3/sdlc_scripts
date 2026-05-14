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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# 1) Read source tables (bronze)
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

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# =========================
# 2) products_silver
# =========================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)         AS product_id,
    CAST(TRIM(pb.product_name) AS STRING)       AS product_name,
    CAST(TRIM(pb.category) AS STRING)           AS category,
    CAST(pb.price AS DOUBLE)                    AS price
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    CAST(price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  CAST(price AS DOUBLE) AS price
FROM dedup
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =========================
# 3) stores_silver
# =========================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING)      AS store_id,
    CAST(TRIM(sb.store_name) AS STRING)    AS store_name,
    CAST(TRIM(sb.state) AS STRING)         AS region
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region
FROM dedup
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =========================
# 4) sales_transactions_silver
# =========================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(tb.transaction_id) AS STRING) AS transaction_id,
    DATE(CAST(tb.transaction_time AS TIMESTAMP)) AS transaction_date,
    CAST(TRIM(tb.store_id) AS STRING)       AS store_id,
    CAST(TRIM(tb.product_id) AS STRING)     AS product_id,
    CAST(tb.quantity AS INT)                AS quantity_sold,
    CAST(tb.sale_amount AS DOUBLE)          AS total_revenue
  FROM transactions_bronze tb
  LEFT JOIN products_silver ps
    ON CAST(TRIM(tb.product_id) AS STRING) = ps.product_id
  LEFT JOIN stores_silver ss
    ON CAST(TRIM(tb.store_id) AS STRING) = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_id
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =========================
# 5) sales_aggregated_silver
# =========================
sales_aggregated_silver_sql = """
SELECT
  sts.transaction_date                 AS date,
  SUM(CAST(sts.total_revenue AS DOUBLE)) AS total_revenue,
  SUM(CAST(sts.quantity_sold AS INT))    AS quantity_sold,
  sts.store_id                        AS store_id,
  sts.product_id                      AS product_id,
  ps.category                         AS category
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.transaction_date,
  sts.store_id,
  sts.product_id,
  ps.category
"""
sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# =========================
# 6) data_quality_silver
# =========================
data_quality_silver_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)           AS data_check,
  CAST(sts.transaction_date AS DATE)           AS check_date,
  CAST(sts.quantity_sold AS BOOLEAN)           AS passed,
  CAST(sts.total_revenue AS INT)               AS errors_count
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""
data_quality_silver_df = spark.sql(data_quality_silver_sql)
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_silver.csv")
)

job.commit()