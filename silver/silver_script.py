import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------------------------------------------------------------
# Glue / Spark Session
# ----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------------------------------------------------------------
# 1) Read source tables from S3
# ----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# ----------------------------------------------------------------------------------
# 2) Create temp views
# ----------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ==================================================================================
# TABLE: products_silver
# ==================================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS product_category,
    CAST(TRIM(pb.brand) AS STRING) AS product_brand,
    CAST(pb.is_active AS BOOLEAN) AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 0 ELSE 1 END,
        CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 0 ELSE 1 END
    ) AS rn
  FROM products_bronze pb
)
SELECT
  CAST(product_id AS STRING) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(product_category AS STRING) AS product_category,
  CAST(product_brand AS STRING) AS product_brand
FROM base
WHERE rn = 1
  AND is_active = true
  AND product_id IS NOT NULL
  AND TRIM(product_id) <> ''
"""
products_silver_df = spark.sql(products_silver_sql)

products_silver_out = products_silver_df.coalesce(1)
products_silver_out.write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/products_silver.csv"
)

# ==================================================================================
# TABLE: stores_silver
# ==================================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(TRIM(sb.state) AS STRING) AS state,
    CAST(TRIM(sb.store_type) AS STRING) AS store_type,
    CAST(sb.open_date AS DATE) AS open_date,
    CAST(TRIM(sb.state) AS STRING) AS store_region,
    CAST(TRIM(sb.store_name) AS STRING) AS store_manager,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.store_type IS NOT NULL AND TRIM(sb.store_type) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.open_date IS NOT NULL THEN 0 ELSE 1 END
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  CAST(store_id AS STRING) AS store_id,
  CAST(store_name AS STRING) AS store_name,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(store_type AS STRING) AS store_type,
  CAST(open_date AS DATE) AS open_date,
  CAST(store_region AS STRING) AS store_region,
  CAST(store_manager AS STRING) AS store_manager
FROM base
WHERE rn = 1
  AND store_id IS NOT NULL
  AND TRIM(store_id) <> ''
"""
stores_silver_df = spark.sql(stores_silver_sql)

stores_silver_out = stores_silver_df.coalesce(1)
stores_silver_out.write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/stores_silver.csv"
)

# ==================================================================================
# TABLE: sales_transactions_silver
# ==================================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS transaction_date,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
)
SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(transaction_date AS DATE) AS transaction_date,
  CAST(store_id AS STRING) AS store_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(quantity_sold AS INT) AS quantity_sold,
  CAST(sales_amount AS DOUBLE) AS sales_amount,
  CAST(transaction_time AS TIMESTAMP) AS transaction_time
FROM base
WHERE rn = 1
  AND transaction_id IS NOT NULL
  AND TRIM(transaction_id) <> ''
  AND store_id IS NOT NULL
  AND TRIM(store_id) <> ''
  AND product_id IS NOT NULL
  AND TRIM(product_id) <> ''
  AND quantity_sold IS NOT NULL
  AND quantity_sold >= 0
  AND sales_amount IS NOT NULL
  AND sales_amount >= 0
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

sales_transactions_silver_out = sales_transactions_silver_df.coalesce(1)
sales_transactions_silver_out.write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)

# ----------------------------------------------------------------------------------
# Read silver outputs back from S3 for downstream join table (as per UDT mapping)
# ----------------------------------------------------------------------------------
products_silver_read_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{TARGET_PATH}/products_silver.csv/")
)
stores_silver_read_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{TARGET_PATH}/stores_silver.csv/")
)
sales_transactions_silver_read_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{TARGET_PATH}/sales_transactions_silver.csv/")
)

products_silver_read_df.createOrReplaceTempView("products_silver")
stores_silver_read_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_read_df.createOrReplaceTempView("sales_transactions_silver")

# ==================================================================================
# TABLE: sales_analysis_silver
# ==================================================================================
sales_analysis_silver_sql = """
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
  CAST(ss.store_region AS STRING) AS store_region,
  CAST(ss.store_manager AS STRING) AS store_manager,
  CAST(ps.product_category AS STRING) AS product_category,
  CAST(ps.product_brand AS STRING) AS product_brand
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
"""
sales_analysis_silver_df = spark.sql(sales_analysis_silver_sql)

sales_analysis_silver_out = sales_analysis_silver_df.coalesce(1)
sales_analysis_silver_out.write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_analysis_silver.csv"
)
