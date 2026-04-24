```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# ------------------------------------------------------------------------------
# Job setup
# ------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"


# ------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")


# ------------------------------------------------------------------------------
# TARGET TABLE: gold_customer_orders
# Sources:
#   silver.customer_orders_silver cos
#   LEFT JOIN silver.customer_order_items_silver cois ON cos.order_id = cois.order_id
# Transformations:
#   - items_count = COUNT(cois.order_id) OVER (PARTITION BY cois.order_id)
#   - (No additional UDT casts specified; apply required SQL funcs via CAST/COALESCE/DATE where relevant)
#   - Ensure one row per order via ROW_NUMBER de-dup
# ------------------------------------------------------------------------------
gold_customer_orders_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(cos.order_id) AS STRING)                                   AS order_id,
    CAST(DATE(cos.order_date) AS DATE)                                   AS order_date,
    CAST(TRIM(cos.customer_id) AS STRING)                                AS customer_id,
    CAST(TRIM(cos.order_status) AS STRING)                               AS order_status,
    CAST(TRIM(cos.order_currency) AS STRING)                             AS order_currency,
    CAST(cos.order_total_amount AS DECIMAL(38, 10))                      AS order_total_amount,
    CAST(COUNT(cois.order_id) OVER (PARTITION BY cois.order_id) AS INT)  AS items_count,
    CAST(cos.source_file_name AS STRING)                                 AS source_file_name,
    CAST(cos.ingested_at AS TIMESTAMP)                                   AS ingested_at
  FROM customer_orders_silver cos
  LEFT JOIN customer_order_items_silver cois
    ON cos.order_id = cois.order_id
),
dedup AS (
  SELECT
    order_id,
    order_date,
    customer_id,
    order_status,
    order_currency,
    order_total_amount,
    COALESCE(items_count, 0) AS items_count,
    source_file_name,
    ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY ingested_at DESC, source_file_name DESC
    ) AS rn
  FROM base
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_currency,
  order_total_amount,
  items_count,
  source_file_name,
  ingested_at
FROM dedup
WHERE rn = 1
"""

gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)


# ------------------------------------------------------------------------------
# TARGET TABLE: gold_customer_order_items
# Sources:
#   silver.customer_order_items_silver cois
# Transformations:
#   - Direct mapping (no joins)
#   - Apply SQL CAST/COALESCE/TRIM per standardization
#   - De-dup with ROW_NUMBER on (order_id, order_item_id)
# ------------------------------------------------------------------------------
gold_customer_order_items_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(cois.order_id) AS STRING)            AS order_id,
    CAST(cois.order_item_id AS INT)                AS order_item_id,
    CAST(TRIM(cois.product_id) AS STRING)          AS product_id,
    CAST(cois.quantity AS INT)                     AS quantity,
    CAST(cois.unit_price AS DECIMAL(38, 10))       AS unit_price,
    CAST(cois.line_amount AS DECIMAL(38, 10))      AS line_amount
  FROM customer_order_items_silver cois
),
dedup AS (
  SELECT
    order_id,
    order_item_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, order_item_id
      ORDER BY order_id, order_item_id
    ) AS rn
  FROM base
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount
FROM dedup
WHERE rn = 1
"""

gold_customer_order_items_df = spark.sql(gold_customer_order_items_sql)

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)


job.commit()
```