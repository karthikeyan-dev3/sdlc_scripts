```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV read options (adjust if your bronze CSVs differ)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "sep": ",",
    "quote": '"',
    "escape": '"',
    "multiLine": "false",
}

# -----------------------------------------------------------------------------------
# 1) SOURCE READS + TEMP VIEWS (Bronze)
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# -----------------------------------------------------------------------------------
# 2) TARGET: currencies_silver (single-row default currency)
#    mapping_details: SELECT 'USD' AS currency_code, TRUE AS is_default
# -----------------------------------------------------------------------------------
currencies_silver_df = spark.sql("""
SELECT
  CAST('USD' AS STRING)  AS currency_code,
  CAST(TRUE  AS BOOLEAN) AS is_default
""")
currencies_silver_df.createOrReplaceTempView("currencies_silver")

(
    currencies_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/currencies_silver.csv")
)

# -----------------------------------------------------------------------------------
# 3) TARGET: customer_orders_silver
#    mapping_details: bronze.customer_orders_bronze cob LEFT JOIN silver.currencies_silver cur ON 1=1
#    Deduplicate by order_id (keep latest by transaction_time)
# -----------------------------------------------------------------------------------
customer_orders_silver_df = spark.sql("""
WITH cur AS (
  -- Constrain to a single DEFAULT row
  SELECT currency_code
  FROM currencies_silver
  WHERE is_default = TRUE
  LIMIT 1
),
dedup AS (
  SELECT
    cob.*,
    ROW_NUMBER() OVER (
      PARTITION BY cob.order_id
      ORDER BY cob.transaction_time DESC
    ) AS rn
  FROM customer_orders_bronze cob
)
SELECT
  CAST(dedup.order_id AS STRING)                         AS order_id,
  CAST(dedup.transaction_time AS DATE)                   AS order_date,
  CAST('UNKNOWN' AS STRING)                              AS customer_id,
  CAST('COMPLETED' AS STRING)                            AS order_status,
  CAST(dedup.sale_amount AS DECIMAL(18,2))               AS order_total_amount,
  CAST(COALESCE(cur.currency_code, 'USD') AS STRING)     AS currency_code
FROM dedup
LEFT JOIN cur
  ON 1 = 1
WHERE dedup.rn = 1
""")
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# -----------------------------------------------------------------------------------
# 4) TARGET: customer_order_items_silver
#    mapping_details: bronze.customer_order_items_bronze coib LEFT JOIN bronze.products_bronze pb ON coib.product_id = pb.product_id
#    De-duplicate identical item rows per (order_id, product_id, quantity, sale_amount).
#    order_item_id: deterministic hash of (order_id, product_id, quantity, sale_amount)
# -----------------------------------------------------------------------------------
customer_order_items_silver_df = spark.sql("""
WITH base AS (
  SELECT
    coib.order_id,
    coib.product_id,
    COALESCE(coib.quantity, 1) AS quantity_raw,
    coib.sale_amount
  FROM customer_order_items_bronze coib
  LEFT JOIN products_bronze pb
    ON coib.product_id = pb.product_id
),
normalized AS (
  SELECT
    CAST(order_id AS STRING)    AS order_id,
    CAST(product_id AS STRING)  AS product_id,
    -- non-negative enforcement
    CASE
      WHEN CAST(quantity_raw AS INT) < 0 THEN 0
      ELSE CAST(quantity_raw AS INT)
    END                        AS quantity,
    CAST(sale_amount AS DECIMAL(18,2)) AS sale_amount
  FROM base
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id, quantity, sale_amount
      ORDER BY product_id
    ) AS rn
  FROM normalized
)
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(
    sha2(
      concat_ws(
        '||',
        COALESCE(order_id, ''),
        COALESCE(product_id, ''),
        CAST(COALESCE(quantity, 0) AS STRING),
        CAST(COALESCE(sale_amount, CAST(0.00 AS DECIMAL(18,2))) AS STRING)
      ),
      256
    ) AS STRING
  ) AS order_item_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(
    CASE
      WHEN COALESCE(quantity, 0) > 0 THEN sale_amount / quantity
      ELSE NULL
    END AS DECIMAL(18,2)
  ) AS unit_price,
  CAST(sale_amount AS DECIMAL(18,2)) AS line_total_amount
FROM dedup
WHERE rn = 1
""")
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

job.commit()
```