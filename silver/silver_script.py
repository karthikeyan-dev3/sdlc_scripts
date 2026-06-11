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

# =============================================================================
# 1) Read source tables (Bronze) and create temp views
# =============================================================================
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

# =============================================================================
# 2) products_silver
# =============================================================================
products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(pb.product_id)    AS product_id,
    TRIM(pb.product_name)  AS product_name,
    TRIM(pb.category)      AS category,
    TRIM(pb.brand)         AS brand,
    CAST(pb.price AS DOUBLE)       AS price,
    CAST(pb.is_active AS BOOLEAN)  AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_name) DESC
    ) AS rn
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
    AND pb.product_name IS NOT NULL
    AND pb.category IS NOT NULL
    AND pb.brand IS NOT NULL
    AND pb.price IS NOT NULL
    AND pb.is_active IS NOT NULL
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM base
WHERE rn = 1
""")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =============================================================================
# 3) stores_silver
# =============================================================================
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(sb.store_id)      AS store_id,
    TRIM(sb.store_name)    AS store_name,
    TRIM(sb.city)          AS city,
    TRIM(sb.state)         AS state,
    TRIM(sb.store_type)    AS store_type,
    CAST(sb.open_date AS DATE) AS open_date,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY CAST(sb.open_date AS DATE) DESC
    ) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
    AND sb.store_name IS NOT NULL
    AND sb.city IS NOT NULL
    AND sb.state IS NOT NULL
    AND sb.store_type IS NOT NULL
    AND sb.open_date IS NOT NULL
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM base
WHERE rn = 1
""")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =============================================================================
# 4) sales_transactions_silver
# =============================================================================
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(stb.transaction_id)                 AS transaction_id,
    TRIM(stb.store_id)                       AS store_id,
    TRIM(stb.product_id)                     AS product_id,
    CAST(stb.quantity AS INT)                AS quantity,
    CAST(stb.sale_amount AS DOUBLE)          AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP)  AS transaction_time,
    CAST(stb.transaction_time AS DATE)       AS txn_date,
    CASE
      WHEN pb.product_id IS NULL OR sb.store_id IS NULL THEN 'REFERENCE_MISSING'
      ELSE 'VALID'
    END AS data_quality_status,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  LEFT JOIN products_bronze pb
    ON stb.product_id = pb.product_id
  LEFT JOIN stores_bronze sb
    ON stb.store_id = sb.store_id
  WHERE stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
    AND stb.quantity IS NOT NULL
    AND stb.sale_amount IS NOT NULL
    AND stb.transaction_time IS NOT NULL
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time
FROM base
WHERE rn = 1
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()