```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Use header + permissive parsing for CSV
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT path format)
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
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

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# ===================================================================================
# TARGET TABLE: sales_transactions_silver
# - Apply EXACT transformations from UDT (incl. CAST(transaction_time AS DATE))
# - Enforce non-null keys and basic data quality
# - Deduplicate by transaction_id keeping latest by transaction_time (ROW_NUMBER)
# ===================================================================================
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING)  AS transaction_id,
    CAST(stb.store_id AS STRING)        AS store_id,
    CAST(stb.product_id AS STRING)      AS product_id,
    CAST(stb.quantity AS INT)           AS quantity,
    CAST(stb.sale_amount AS DECIMAL(38,10)) AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(stb.transaction_time AS DATE)  AS sales_date
  FROM sales_transactions_bronze stb
),
filtered AS (
  SELECT *
  FROM base
  WHERE transaction_id IS NOT NULL
    AND store_id IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity IS NOT NULL AND quantity > 0
    AND sale_amount IS NOT NULL AND sale_amount >= 0
    AND transaction_time IS NOT NULL
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time,
  sales_date
FROM dedup
WHERE rn = 1
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ===================================================================================
# TARGET TABLE: stores_silver
# - Apply UDT mappings + standardization using TRIM/UPPER/LOWER/COALESCE
# - Fill missing country with default 'USA'
# - Deduplicate on store_id keeping most recent by open_date (ROW_NUMBER)
# ===================================================================================
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,

    -- Standardize text fields
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.city)       AS city,
    UPPER(TRIM(sb.state)) AS state,

    COALESCE(TRIM(sb.country), 'USA') AS country,

    LOWER(TRIM(sb.store_type)) AS store_type,

    CAST(sb.open_date AS DATE) AS open_date
  FROM stores_bronze sb
),
filtered AS (
  SELECT *
  FROM base
  WHERE store_id IS NOT NULL
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date DESC
    ) AS rn
  FROM filtered
)
SELECT
  store_id,
  store_name,
  city,
  state,
  country,
  store_type,
  open_date
FROM dedup
WHERE rn = 1
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ===================================================================================
# TARGET TABLE: products_silver
# - Apply UDT mappings + standardization using TRIM/UPPER/LOWER/COALESCE
# - If is_active exists, keep only is_active = true; else keep all
#   (Implemented without if/else by treating NULL as TRUE via COALESCE)
# - Deduplicate on product_id (ROW_NUMBER)
# ===================================================================================
products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,

    TRIM(pb.product_name) AS product_name,
    TRIM(pb.brand)        AS brand,

    -- Standardize category naming
    LOWER(TRIM(pb.category)) AS category,

    CAST(pb.price AS DECIMAL(38,10)) AS price,
    CAST(pb.is_active AS BOOLEAN) AS is_active
  FROM products_bronze pb
),
filtered AS (
  SELECT *
  FROM base
  WHERE product_id IS NOT NULL
    AND COALESCE(is_active, TRUE) = TRUE
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id
    ) AS rn
  FROM filtered
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
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

job.commit()
```