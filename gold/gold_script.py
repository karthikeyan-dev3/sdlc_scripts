import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------
store_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
store_master_silver_df.createOrReplaceTempView("store_master_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# --------------------------------------------------------------------
# TARGET: gold_store_master
# --------------------------------------------------------------------
gold_store_master_sql = """
SELECT
  CAST(sms.store_id AS STRING)   AS store_id,
  CAST(sms.store_name AS STRING) AS store_name,
  CAST(sms.region AS STRING)     AS region,
  CAST(sms.store_type AS STRING) AS store_type
FROM store_master_silver sms
"""

gold_store_master_df = spark.sql(gold_store_master_sql)

(
    gold_store_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_product_master
# --------------------------------------------------------------------
gold_product_master_sql = """
SELECT
  CAST(pms.product_id AS STRING)   AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING)     AS category,
  CAST(pms.brand AS STRING)        AS brand
FROM product_master_silver pms
"""

gold_product_master_df = spark.sql(gold_product_master_sql)

(
    gold_product_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_sales_performance
# --------------------------------------------------------------------
gold_sales_performance_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)      AS transaction_id,
  CAST(sts.product_id AS STRING)          AS product_id,
  CAST(sts.store_id AS STRING)            AS store_id,
  CAST(sts.transaction_date AS DATE)      AS transaction_date,
  CAST(sts.total_revenue AS DOUBLE)       AS total_revenue,
  CAST(sts.quantity_sold AS INT)          AS quantity_sold,
  CAST(1 AS INT)                          AS total_transactions
FROM sales_transactions_silver sts
INNER JOIN product_master_silver pms
  ON sts.product_id = pms.product_id
INNER JOIN store_master_silver sms
  ON sts.store_id = sms.store_id
"""

gold_sales_performance_df = spark.sql(gold_sales_performance_sql)

(
    gold_sales_performance_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_product_performance
# --------------------------------------------------------------------
gold_product_performance_sql = """
WITH base AS (
  SELECT
    CAST(pms.product_id AS STRING)   AS product_id,
    CAST(pms.product_name AS STRING) AS product_name,
    CAST(pms.category AS STRING)     AS category,
    CAST(sts.total_revenue AS DOUBLE) AS total_revenue
  FROM sales_transactions_silver sts
  INNER JOIN product_master_silver pms
    ON sts.product_id = pms.product_id
),
agg AS (
  SELECT
    product_id,
    product_name,
    category,
    SUM(total_revenue) AS revenue_contribution
  FROM base
  GROUP BY product_id, product_name, category
)
SELECT
  product_id,
  product_name,
  category,
  CAST(revenue_contribution AS DOUBLE) AS revenue_contribution,
  CASE
    WHEN DENSE_RANK() OVER (ORDER BY revenue_contribution DESC) = 1 THEN 'Y'
    ELSE 'N'
  END AS top_performing_indicator
FROM agg
"""

gold_product_performance_df = spark.sql(gold_product_performance_sql)

(
    gold_product_performance_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)