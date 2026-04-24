```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read source tables from S3 (STRICT PATH FORMAT)
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create temp views
# -----------------------------------------------------------------------------------
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# ===================================================================================
# Target table: silver.customer_orders_silver
# ===================================================================================
# Transformations (EXACT per UDT):
# - order_id = cob.transaction_id (TRIM + filter null/blank)
# - order_date = CAST(cob.transaction_time AS DATE)
# - order_total_amount = COALESCE(cob.sale_amount, 0)
# - order_status = 'completed'
# - order_currency = 'USD'
# - customer_id = cob.store_id
# - items_count = COUNT(*) by transaction_id from customer_order_items_bronze
# - source_file_name = cob.source_file_name
# - ingested_at = cob.ingested_at
# - Dedupe to one row per order_id (transaction_id) using ROW_NUMBER
customer_orders_silver_sql = """
WITH cob_clean AS (
  SELECT
    TRIM(cob.transaction_id)                          AS transaction_id,
    cob.transaction_time                              AS transaction_time,
    cob.store_id                                      AS store_id,
    cob.sale_amount                                   AS sale_amount,
    cob.source_file_name                              AS source_file_name,
    cob.ingested_at                                   AS ingested_at
  FROM customer_orders_bronze cob
  WHERE cob.transaction_id IS NOT NULL
    AND TRIM(cob.transaction_id) <> ''
),
items_count_by_order AS (
  SELECT
    TRIM(coib.transaction_id)                         AS transaction_id,
    COUNT(*)                                          AS items_count
  FROM customer_order_items_bronze coib
  WHERE coib.transaction_id IS NOT NULL
    AND TRIM(coib.transaction_id) <> ''
  GROUP BY TRIM(coib.transaction_id)
),
dedup AS (
  SELECT
    transaction_id,
    transaction_time,
    store_id,
    sale_amount,
    source_file_name,
    ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY ingested_at DESC, source_file_name DESC
    ) AS rn
  FROM cob_clean
)
SELECT
  d.transaction_id                                    AS order_id,
  CAST(d.transaction_time AS DATE)                    AS order_date,
  d.store_id                                          AS customer_id,
  'completed'                                         AS order_status,
  'USD'                                               AS order_currency,
  COALESCE(d.sale_amount, 0)                          AS order_total_amount,
  COALESCE(ic.items_count, 0)                         AS items_count,
  d.source_file_name                                  AS source_file_name,
  d.ingested_at                                       AS ingested_at
FROM dedup d
LEFT JOIN items_count_by_order ic
  ON d.transaction_id = ic.transaction_id
WHERE d.rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (STRICT OUTPUT RULE)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ===================================================================================
# Target table: silver.customer_order_items_silver
# ===================================================================================
# Transformations (EXACT per UDT):
# - Filter out null/blank transaction_id
# - order_id = coib.transaction_id (TRIM)
# - order_item_id = ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY COALESCE(product_id,'') , COALESCE(sale_amount,0) , COALESCE(quantity,0))
# - product_id = TRIM(coib.product_id)
# - quantity = COALESCE(coib.quantity, 0)
# - line_amount = COALESCE(coib.sale_amount, 0)
# - unit_price = CASE WHEN COALESCE(quantity,0)>0 THEN COALESCE(sale_amount,0)/quantity ELSE pb.price END
# - Dedupe to one row per (order_id, order_item_id) (naturally unique via row_number within order_id)
customer_order_items_silver_sql = """
WITH base AS (
  SELECT
    TRIM(coib.transaction_id)                          AS transaction_id,
    coib.product_id                                    AS product_id_raw,
    coib.sale_amount                                   AS sale_amount,
    coib.quantity                                      AS quantity
  FROM customer_order_items_bronze coib
  WHERE coib.transaction_id IS NOT NULL
    AND TRIM(coib.transaction_id) <> ''
),
joined AS (
  SELECT
    b.transaction_id,
    TRIM(b.product_id_raw)                             AS product_id,
    COALESCE(b.quantity, 0)                            AS quantity,
    COALESCE(b.sale_amount, 0)                         AS line_amount,
    pb.price                                           AS pb_price
  FROM base b
  LEFT JOIN products_bronze pb
    ON TRIM(b.product_id_raw) = pb.product_id
),
numbered AS (
  SELECT
    transaction_id                                     AS order_id,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY
        COALESCE(product_id, ''),
        COALESCE(line_amount, 0),
        COALESCE(quantity, 0)
    )                                                  AS order_item_id,
    product_id                                         AS product_id,
    quantity                                           AS quantity,
    CASE
      WHEN COALESCE(quantity, 0) > 0 THEN COALESCE(line_amount, 0) / quantity
      ELSE pb_price
    END                                                AS unit_price,
    COALESCE(line_amount, 0)                           AS line_amount
  FROM joined
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount
FROM numbered
"""

customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (STRICT OUTPUT RULE)
(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

job.commit()
```