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

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
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

# -----------------------------
# 2) Create temp views
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# TABLE: silver.product_silver
# ============================================================
product_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    CAST(pb.price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM ranked
WHERE rn = 1
"""

product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ============================================================
# TABLE: silver.store_silver
# ============================================================
store_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CONCAT(CAST(TRIM(sb.city) AS STRING), ', ', CAST(TRIM(sb.state) AS STRING)) AS location,
    CAST(TRIM(sb.state) AS STRING) AS region,
    CAST(TRIM(sb.store_type) AS STRING) AS store_manager,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN sb.store_type IS NOT NULL AND TRIM(sb.store_type) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  location,
  region,
  store_manager
FROM ranked
WHERE rn = 1
"""

store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ============================================================
# TABLE: silver.sales_silver
# ============================================================
sales_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS date,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS sales_amount
  FROM sales_transactions_bronze stb
  INNER JOIN product_silver ps
    ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
  INNER JOIN store_silver ss
    ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
  WHERE
    stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
    AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    date,
    product_id,
    store_id,
    quantity_sold,
    sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  date,
  product_id,
  store_id,
  quantity_sold,
  sales_amount
FROM dedup
WHERE rn = 1
"""

sales_silver_df = spark.sql(sales_silver_sql)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# ============================================================
# TABLE: silver.sales_aggregation_silver
# ============================================================
sales_aggregation_silver_sql = """
SELECT
  sls.date AS date,
  sls.store_id AS store_id,
  sls.product_id AS product_id,
  CAST(SUM(sls.quantity_sold) AS INT) AS total_quantity_sold,
  SUM(sls.sales_amount) AS total_sales_amount,
  CASE
    WHEN SUM(sls.quantity_sold) = 0 THEN NULL
    ELSE SUM(sls.sales_amount) / SUM(sls.quantity_sold)
  END AS average_price
FROM sales_silver sls
GROUP BY
  sls.date,
  sls.store_id,
  sls.product_id
"""

sales_aggregation_silver_df = spark.sql(sales_aggregation_silver_sql)
sales_aggregation_silver_df.createOrReplaceTempView("sales_aggregation_silver")

(
    sales_aggregation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregation_silver.csv")
)

job.commit()