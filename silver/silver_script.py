
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

# ---------------------------
# 1) Read source tables
# ---------------------------
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

# ---------------------------
# 2) Create temp views
# ---------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# TABLE: silver.product_master_silver
# ============================================================
product_master_silver_sql = """
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    CAST(pb.product_name AS STRING) AS product_name,
    CAST(pb.category AS STRING) AS category,
    CAST(pb.price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(pb.product_id AS STRING)
      ORDER BY CAST(pb.product_id AS STRING)
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM base
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ============================================================
# TABLE: silver.store_master_silver
# ============================================================
store_master_silver_sql = """
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    CAST(sb.store_name AS STRING) AS store_name,
    CONCAT(CAST(sb.city AS STRING), ', ', CAST(sb.state AS STRING)) AS store_location,
    CAST(sb.state AS STRING) AS region,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sb.store_id AS STRING)
      ORDER BY CAST(sb.store_id AS STRING)
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  store_location,
  region
FROM base
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ============================================================
# TABLE: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(stb.transaction_id AS STRING)
      ORDER BY CAST(stb.transaction_id AS STRING)
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON CAST(stb.product_id AS STRING) = pms.product_id
  INNER JOIN store_master_silver sms
    ON CAST(stb.store_id AS STRING) = sms.store_id
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_revenue
FROM base
WHERE rn = 1
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

# ============================================================
# TABLE: silver.daily_sales_aggregates_silver
# ============================================================
daily_sales_aggregates_silver_sql = """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.transaction_date AS DATE) AS date,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CASE
    WHEN SUM(CAST(sts.quantity_sold AS INT)) > 0
      THEN CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS INT)) AS DOUBLE)
  END AS average_price
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING),
  CAST(sts.transaction_date AS DATE)
"""

daily_sales_aggregates_silver_df = spark.sql(daily_sales_aggregates_silver_sql)

(
    daily_sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregates_silver.csv")
)

job.commit()