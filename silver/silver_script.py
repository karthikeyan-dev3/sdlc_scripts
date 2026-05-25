import sys
from awsglue.transforms import *
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

# =============================================================================
# Read Source Tables (Bronze)
# =============================================================================
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# =============================================================================
# Create Temp Views
# =============================================================================
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =============================================================================
# Target: silver.products_silver
# =============================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(pb.product_id), '') AS STRING) AS product_id,
    CAST(NULLIF(TRIM(pb.product_name), '') AS STRING) AS product_name,
    CAST(NULLIF(TRIM(pb.category), '') AS STRING) AS category
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
  WHERE product_id IS NOT NULL
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
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# =============================================================================
# Target: silver.stores_silver
# =============================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(sb.store_id), '') AS STRING) AS store_id,
    CONCAT(
      COALESCE(NULLIF(TRIM(sb.city), ''), ''),
      ', ',
      COALESCE(NULLIF(TRIM(sb.state), ''), '')
    ) AS store_location,
    CAST(NULLIF(TRIM(sb.store_type), '') AS STRING) AS store_type,
    CAST(NULLIF(TRIM(sb.state), '') AS STRING) AS region
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_location,
    store_type,
    region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_location IS NOT NULL AND TRIM(store_location) <> ', ' THEN 1 ELSE 0 END +
        CASE WHEN store_type IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
  WHERE store_id IS NOT NULL
)
SELECT
  store_id,
  store_location,
  store_type,
  region
FROM dedup
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# =============================================================================
# Target: silver.sales_transactions_silver
# =============================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(stb.transaction_id), '') AS STRING) AS transaction_id,
    CAST(NULLIF(TRIM(stb.product_id), '') AS STRING) AS product_id,
    CAST(NULLIF(TRIM(stb.store_id), '') AS STRING) AS store_id,
    DATE(stb.transaction_time) AS sale_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
),
validated AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    CASE WHEN quantity_sold < 0 THEN NULL ELSE quantity_sold END AS quantity_sold,
    CASE WHEN total_sales_amount < 0 THEN NULL ELSE total_sales_amount END AS total_sales_amount,
    transaction_time
  FROM base
  WHERE transaction_id IS NOT NULL
),
joined AS (
  SELECT
    v.transaction_id,
    v.product_id,
    v.store_id,
    v.sale_date,
    v.quantity_sold,
    v.total_sales_amount,
    v.transaction_time
  FROM validated v
  LEFT JOIN products_silver ps ON v.product_id = ps.product_id
  LEFT JOIN stores_silver ss ON v.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount,
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
  sale_date,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()