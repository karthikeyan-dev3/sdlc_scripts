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

# ---------------------------------------------------------------------
# Read Source Tables (Bronze)
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Temp Views
# ---------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ---------------------------------------------------------------------
# TARGET: silver.product_master_silver
# ---------------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    CAST(pb.price AS DOUBLE) AS product_price
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  product_category,
  product_price
FROM dedup
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ---------------------------------------------------------------------
# TARGET: silver.store_master_silver
# ---------------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.state) AS store_region,
    TRIM(sb.store_type) AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_region,
    store_type,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  store_region,
  store_type
FROM dedup
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ---------------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# ---------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(stb.quantity AS INT) AS quantity_sold
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON TRIM(stb.product_id) = pms.product_id
  INNER JOIN store_master_silver sms
    ON TRIM(stb.store_id) = sms.store_id
  WHERE
    TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
    AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) > 0
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    sale_amount,
    quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM base
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  sale_amount,
  quantity_sold
FROM dedup
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()