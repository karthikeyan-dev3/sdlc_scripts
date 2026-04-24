```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Read SOURCE tables from S3 (Bronze)
# IMPORTANT: Path must be constructed using FILE_FORMAT and end with "/"
# --------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ======================================================================================
# TARGET TABLE: stores_silver
# - Clean text fields (trim / casing)
# - Filter out null/blank store_id
# - De-duplicate using ROW_NUMBER by store_id; latest by open_date desc (then store_name desc as stable tie-breaker)
# - Output columns: store_id, store_name, city, state, store_type, open_date
# ======================================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(sb.store_id AS STRING))                                   AS store_id,
    TRIM(CAST(sb.store_name AS STRING))                                 AS store_name,
    TRIM(CAST(sb.city AS STRING))                                       AS city,
    UPPER(TRIM(CAST(sb.state AS STRING)))                               AS state,
    UPPER(TRIM(CAST(sb.store_type AS STRING)))                          AS store_type,
    CAST(sb.open_date AS DATE)                                          AS open_date
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
    AND TRIM(CAST(sb.store_id AS STRING)) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date DESC NULLS LAST, store_name DESC NULLS LAST
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

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# Create temp view for downstream join usage
stores_silver_df.createOrReplaceTempView("stores_silver")

# ======================================================================================
# TARGET TABLE: products_silver
# - Clean text fields (trim / casing)
# - Enforce non-null/blank product_id
# - De-duplicate using ROW_NUMBER by product_id:
#     latest by is_active desc, then price desc (then product_name desc as stable tie-breaker)
# - Output columns: product_id, product_name, brand, category, price, is_active
# ======================================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(pb.product_id AS STRING))                                 AS product_id,
    TRIM(CAST(pb.product_name AS STRING))                               AS product_name,
    TRIM(CAST(pb.brand AS STRING))                                      AS brand,
    LOWER(TRIM(CAST(pb.category AS STRING)))                            AS category,
    CAST(pb.price AS DECIMAL(18,2))                                     AS price,
    CAST(pb.is_active AS BOOLEAN)                                       AS is_active
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
    AND TRIM(CAST(pb.product_id AS STRING)) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        COALESCE(CAST(is_active AS INT), 0) DESC,
        price DESC NULLS LAST,
        product_name DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  brand,
  category,
  price,
  is_active
FROM dedup
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# Create temp view for downstream join usage
products_silver_df.createOrReplaceTempView("products_silver")

# ======================================================================================
# TARGET TABLE: sales_transactions_silver
# - Derive sales_date = CAST(transaction_time AS DATE)
# - Standardize IDs (trim)
# - Remove null/blank transaction_id/store_id/product_id
# - De-duplicate by transaction_id keeping latest transaction_time
# - Enforce numeric validity: quantity >= 0, sale_amount >= 0
# - Retain only transactions that match conformed store/product dims (inner joins)
# - Output: transaction_id, sales_date, transaction_time, store_id, product_id, quantity, sale_amount
# ======================================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(stb.transaction_id AS STRING))                             AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP)                              AS transaction_time,
    CAST(stb.transaction_time AS DATE)                                   AS sales_date,
    TRIM(CAST(stb.store_id AS STRING))                                   AS store_id,
    TRIM(CAST(stb.product_id AS STRING))                                 AS product_id,
    CAST(stb.quantity AS INT)                                            AS quantity,
    CAST(stb.sale_amount AS DECIMAL(18,2))                               AS sale_amount
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
    AND TRIM(CAST(stb.transaction_id AS STRING)) <> ''
    AND stb.store_id IS NOT NULL
    AND TRIM(CAST(stb.store_id AS STRING)) <> ''
    AND stb.product_id IS NOT NULL
    AND TRIM(CAST(stb.product_id AS STRING)) <> ''
),
validated AS (
  SELECT
    *
  FROM base
  WHERE COALESCE(quantity, 0) >= 0
    AND COALESCE(sale_amount, CAST(0.00 AS DECIMAL(18,2))) >= 0
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC NULLS LAST
    ) AS rn
  FROM validated
),
joined AS (
  SELECT
    d.transaction_id,
    d.sales_date,
    d.transaction_time,
    d.store_id,
    d.product_id,
    d.quantity,
    d.sale_amount
  FROM dedup d
  INNER JOIN stores_silver ss
    ON d.store_id = ss.store_id
  INNER JOIN products_silver ps
    ON d.product_id = ps.product_id
  WHERE d.rn = 1
)
SELECT
  transaction_id,
  sales_date,
  transaction_time,
  store_id,
  product_id,
  quantity,
  sale_amount
FROM joined
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
```