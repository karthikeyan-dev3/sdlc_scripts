```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
#    IMPORTANT: path format must be: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------
transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# =================================================================================
# TARGET TABLE: silver.transactions_silver
# =================================================================================
transactions_silver_sql = """
WITH base AS (
  SELECT
    -- Exact UDT transformations
    CAST(DATE(tb.transaction_time) AS DATE)                          AS sales_date,
    CAST(tb.transaction_id AS STRING)                                AS transaction_id,
    CAST(tb.store_id AS STRING)                                      AS store_id,
    CAST(tb.product_id AS STRING)                                    AS product_id,
    CAST(tb.quantity AS INT)                                         AS quantity,
    CAST(tb.sale_amount AS DECIMAL(38, 10))                          AS sale_amount,
    TRIM(sb.store_name)                                              AS store_name,
    TRIM(sb.city)                                                    AS city,
    TRIM(sb.state)                                                   AS state,
    TRIM(sb.store_type)                                              AS store_type,
    TRIM(pb.product_name)                                            AS product_name,
    TRIM(pb.brand)                                                   AS brand,
    UPPER(TRIM(pb.category))                                         AS category,

    -- For dedup ordering and filtering
    tb.transaction_time                                              AS transaction_time,
    pb.is_active                                                     AS is_active,

    ROW_NUMBER() OVER (
      PARTITION BY tb.transaction_id
      ORDER BY tb.transaction_time DESC
    ) AS rn
  FROM transactions_bronze tb
  LEFT JOIN stores_bronze sb
    ON tb.store_id = sb.store_id
  LEFT JOIN products_bronze pb
    ON tb.product_id = pb.product_id
),
filtered AS (
  SELECT
    sales_date,
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    store_name,
    city,
    state,
    store_type,
    product_name,
    brand,
    category
  FROM base
  WHERE 1=1
    -- Non-null constraints (UDT: not_accepted)
    AND transaction_id IS NOT NULL
    AND store_id IS NOT NULL
    AND product_id IS NOT NULL

    -- Dedup keep latest
    AND rn = 1

    -- Business rules from description: coerce null metrics to 0 and exclude negatives
    AND COALESCE(quantity, 0) >= 0
    AND COALESCE(sale_amount, CAST(0 AS DECIMAL(38, 10))) >= CAST(0 AS DECIMAL(38, 10))

    -- Include only active products when pb.is_active is true
    AND COALESCE(is_active, false) = true
)
SELECT
  sales_date,
  transaction_id,
  store_id,
  product_id,
  CAST(COALESCE(quantity, 0) AS INT)                                 AS quantity,
  CAST(COALESCE(sale_amount, CAST(0 AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS sale_amount,
  store_name,
  city,
  state,
  store_type,
  product_name,
  brand,
  category
FROM filtered
"""

transactions_silver_df = spark.sql(transactions_silver_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# =================================================================================
# TARGET TABLE: silver.stores_silver
# =================================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    -- Exact UDT transformations
    CAST(sb.store_id AS STRING)                       AS store_id,
    TRIM(sb.store_name)                               AS store_name,
    TRIM(sb.city)                                     AS city,
    TRIM(sb.state)                                    AS state,
    TRIM(sb.store_type)                               AS store_type,
    CAST(sb.open_date AS DATE)                        AS open_date,
    CAST('USA' AS STRING)                             AS country,

    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.open_date DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date,
  country
FROM base
WHERE 1=1
  -- Non-null constraint (UDT: not_accepted)
  AND store_id IS NOT NULL
  -- Dedup keep latest
  AND rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =================================================================================
# TARGET TABLE: silver.products_silver
# =================================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    -- Exact UDT transformations
    CAST(pb.product_id AS STRING)                      AS product_id,
    TRIM(pb.product_name)                              AS product_name,
    TRIM(pb.brand)                                     AS brand,
    UPPER(TRIM(pb.category))                            AS category,
    CAST(pb.price AS DECIMAL(38, 10))                  AS price,
    CAST(pb.is_active AS BOOLEAN)                      AS is_active,

    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  brand,
  category,
  price,
  is_active
FROM base
WHERE 1=1
  -- Non-null constraint (UDT: not_accepted)
  AND product_id IS NOT NULL
  -- Dedup keep one record per product_id
  AND rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =================================================================================
# TARGET TABLE: silver.categories_silver
# =================================================================================
categories_silver_sql = """
SELECT DISTINCT
  -- Exact UDT transformation
  UPPER(TRIM(pb.category)) AS category
FROM products_bronze pb
WHERE 1=1
  AND pb.category IS NOT NULL
  AND TRIM(pb.category) <> ''
"""

categories_silver_df = spark.sql(categories_silver_sql)

(
    categories_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/categories_silver.csv")
)
```