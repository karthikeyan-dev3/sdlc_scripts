```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Read Source Tables (Bronze)
# --------------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------
# Target: products_silver
# --------------------------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS DOUBLE) AS price
  FROM products_bronze pb
  WHERE
    pb.product_id IS NOT NULL
    AND TRIM(pb.product_id) <> ''
    AND CAST(pb.is_active AS BOOLEAN) = TRUE
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM dedup
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: stores_silver
# --------------------------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    TRIM(sb.store_type) AS store_type
  FROM stores_bronze sb
  WHERE
    sb.store_id IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    store_type,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  location,
  store_type
FROM dedup
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: sales_transactions_silver
# --------------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
    CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
    CAST(stb.quantity AS INT) AS quantity_sold
  FROM sales_transactions_bronze stb
  WHERE
    stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
    AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
    AND stb.transaction_time IS NOT NULL AND TRIM(stb.transaction_time) <> ''
),
validated AS (
  SELECT *
  FROM base
  WHERE
    quantity_sold > 0
    AND sales_amount >= 0
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_time,
    transaction_date,
    sales_amount,
    quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
  FROM validated
)
SELECT
  d.transaction_id,
  d.product_id,
  d.store_id,
  d.transaction_date,
  d.sales_amount,
  d.quantity_sold
FROM dedup d
INNER JOIN products_silver ps
  ON d.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON d.store_id = ss.store_id
WHERE d.rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: aggregated_sales_daily_silver
# --------------------------------------------------------------------------------------
aggregated_sales_daily_silver_sql = """
SELECT
  sts.transaction_date AS date,
  SUM(sts.sales_amount) AS total_sales,
  SUM(sts.quantity_sold) AS total_quantity,
  CASE
    WHEN SUM(sts.quantity_sold) = 0 THEN NULL
    ELSE SUM(sts.sales_amount) / SUM(sts.quantity_sold)
  END AS average_price,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions
FROM sales_transactions_silver sts
GROUP BY sts.transaction_date
"""
aggregated_sales_daily_silver_df = spark.sql(aggregated_sales_daily_silver_sql)

(
    aggregated_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_daily_silver.csv")
)

job.commit()
```