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

# ------------------------------------------------------------------------------
# 1) Read source tables (Bronze) and create temp views
# ------------------------------------------------------------------------------

product_master_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")

store_master_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------
# 2) product_master_silver
#    - Standardize product_name/category (trim/upper)
#    - Cast price to double
#    - Keep latest record per product_id if duplicates exist (ROW_NUMBER)
#    - Filter to active products (is_active = true)
# ------------------------------------------------------------------------------

product_master_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    p.product_id AS product_id,
    UPPER(TRIM(p.product_name)) AS product_name,
    UPPER(TRIM(p.category)) AS product_category,
    CAST(p.price AS DOUBLE) AS product_price,
    ROW_NUMBER() OVER (
      PARTITION BY p.product_id
      ORDER BY p.product_id
    ) AS rn
  FROM product_master_bronze p
  WHERE p.is_active = true
)
SELECT
  product_id,
  product_name,
  product_category,
  product_price
FROM ranked
WHERE rn = 1
""")
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) store_master_silver
#    - Standardize store_name/city/state (trim)
#    - Derive store_location = 'city, state'
#    - store_manager = NULL
#    - Keep latest record per store_id if duplicates exist (ROW_NUMBER)
# ------------------------------------------------------------------------------

store_master_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    s.store_id AS store_id,
    TRIM(s.store_name) AS store_name,
    CONCAT(TRIM(s.city), ', ', TRIM(s.state)) AS store_location,
    CAST(NULL AS STRING) AS store_manager,
    ROW_NUMBER() OVER (
      PARTITION BY s.store_id
      ORDER BY s.store_id
    ) AS rn
  FROM store_master_bronze s
)
SELECT
  store_id,
  store_name,
  store_location,
  store_manager
FROM ranked
WHERE rn = 1
""")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------------------------
# 4) sales_transactions_silver
#    - De-duplicate by transaction_id (keep latest by transaction_time)
#    - Enforce referential integrity via joins to product/store silver
#    - Derive transaction_date = CAST(transaction_time AS DATE)
#    - Clean measures: COALESCE quantity/sale_amount to 0
#    - Filter out negative quantity/sale_amount
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    t.transaction_id AS transaction_id,
    t.store_id AS store_id,
    t.product_id AS product_id,
    t.transaction_time AS transaction_time,
    CAST(t.transaction_time AS DATE) AS transaction_date,
    COALESCE(t.quantity, 0) AS quantity,
    COALESCE(t.sale_amount, 0) AS sale_amount,
    ROW_NUMBER() OVER (
      PARTITION BY t.transaction_id
      ORDER BY t.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze t
  INNER JOIN product_master_silver pm
    ON t.product_id = pm.product_id
  INNER JOIN store_master_silver sm
    ON t.store_id = sm.store_id
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_time,
  transaction_date,
  quantity,
  sale_amount
FROM base
WHERE rn = 1
  AND quantity >= 0
  AND sale_amount >= 0
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()