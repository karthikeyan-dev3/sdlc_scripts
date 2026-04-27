```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# CSV read options (adjust if your bronze files do not have headers)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\"",
}

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# -----------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ===================================================================================
# TABLE: silver.store_dim_silver
# - Clean text fields: TRIM + casing standardization
# - Dedup by store_id, keep latest by open_date, deterministic tie-breaker
# - Add constant country = 'USA'
# ===================================================================================
store_dim_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(UPPER(TRIM(sb.state)) AS STRING) AS state,
    CAST(UPPER(TRIM(sb.store_type)) AS STRING) AS store_type,
    CAST(sb.open_date AS DATE) AS open_date,
    CAST('USA' AS STRING) AS country
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date DESC, store_name DESC
    ) AS rn
  FROM base
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
"""

store_dim_silver_df = spark.sql(store_dim_silver_sql)
store_dim_silver_df.createOrReplaceTempView("store_dim_silver")

# Write: SINGLE CSV file directly under TARGET_PATH
(
    store_dim_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_dim_silver.csv")
)

# ===================================================================================
# TABLE: silver.product_dim_silver
# - Clean text fields: TRIM + casing standardization
# - Dedup by product_id: prefer is_active = true, then deterministic tie-breaker
# ===================================================================================
product_dim_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(UPPER(TRIM(pb.brand)) AS STRING) AS brand,
    CAST(UPPER(TRIM(pb.category)) AS STRING) AS category,
    CAST(pb.price AS DECIMAL(18,2)) AS price,
    CAST(COALESCE(pb.is_active, false) AS BOOLEAN) AS is_active
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN is_active = true THEN 1 ELSE 0 END DESC,
        product_name DESC
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

product_dim_silver_df = spark.sql(product_dim_silver_sql)
product_dim_silver_df.createOrReplaceTempView("product_dim_silver")

# Write: SINGLE CSV file directly under TARGET_PATH
(
    product_dim_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_dim_silver.csv")
)

# ===================================================================================
# TABLE: silver.sales_transactions_silver
# - Dedup by transaction_id (keep latest by transaction_time)
# - Derive sales_date = CAST(transaction_time AS DATE)
# - Standardize types + filter invalid (non-positive quantity or sale_amount)
# - Join to conformed store/product dimensions
# ===================================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sales_date,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DECIMAL(18,2)) AS sale_amount
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
),
valid AS (
  SELECT
    transaction_id,
    sales_date,
    transaction_time,
    store_id,
    product_id,
    quantity,
    sale_amount
  FROM dedup
  WHERE rn = 1
    AND quantity IS NOT NULL AND quantity > 0
    AND sale_amount IS NOT NULL AND sale_amount > 0
)
SELECT
  v.transaction_id,
  v.sales_date,
  v.transaction_time,
  v.store_id,
  sds.store_name,
  sds.city,
  sds.state,
  sds.country,
  sds.store_type,
  v.product_id,
  pds.product_name,
  pds.brand,
  pds.category,
  v.quantity,
  v.sale_amount
FROM valid v
LEFT JOIN store_dim_silver sds
  ON v.store_id = sds.store_id
LEFT JOIN product_dim_silver pds
  ON v.product_id = pds.product_id
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

# Write: SINGLE CSV file directly under TARGET_PATH
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
```