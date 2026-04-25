```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ======================================================================================
# AWS Glue bootstrap
# ======================================================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ======================================================================================
# Parameters (as provided)
# ======================================================================================
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ======================================================================================
# Read source tables from S3 (STRICT PATH FORMAT)
# ======================================================================================
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

etl_load_audit_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_load_audit_orders_bronze.{FILE_FORMAT}/")
)
etl_load_audit_orders_bronze_df.createOrReplaceTempView("etl_load_audit_orders_bronze")

# ======================================================================================
# Target: silver.customer_orders_silver
# - De-duplicate by order_id keeping latest order_time
# - Filter invalid: order_id null/blank OR total_amount null
# ======================================================================================
customer_orders_silver_sql = """
WITH base AS (
  SELECT
    cob.order_id,
    cob.order_time,
    cob.store_id,
    cob.total_amount
  FROM customer_orders_bronze cob
  WHERE cob.order_id IS NOT NULL
    AND TRIM(cob.order_id) <> ''
    AND cob.total_amount IS NOT NULL
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_time DESC
    ) AS rn
  FROM base
)
SELECT
  cob.order_id                                                AS order_id,
  CAST(cob.order_time AS DATE)                                AS order_date,
  cob.store_id                                                AS customer_id,
  'COMPLETED'                                                 AS order_status,
  CAST(cob.total_amount AS DECIMAL(18,2))                     AS order_total_amount,
  'USD'                                                       AS currency_code,
  'sales_transactions_raw'                                    AS source_system,
  CURRENT_TIMESTAMP                                           AS ingested_at
FROM dedup cob
WHERE cob.rn = 1
"""
customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ======================================================================================
# Target: silver.customer_order_items_silver
# - De-duplicate by (order_id, product_id) keeping latest ingested record if duplicates exist
#   (using a deterministic fallback: prefer latest line_amount, then quantity, then product_name)
# - Filter invalid: order_id/product_id null/blank
# - Enforce non-negative quantity and amounts
# ======================================================================================
customer_order_items_silver_sql = """
WITH base AS (
  SELECT
    coib.order_id,
    coib.product_id,
    coib.quantity,
    coib.line_amount,
    pb.product_id AS pb_product_id,
    pb.product_name
  FROM customer_order_items_bronze coib
  LEFT JOIN products_bronze pb
    ON coib.product_id = pb.product_id
  WHERE coib.order_id IS NOT NULL
    AND TRIM(coib.order_id) <> ''
    AND coib.product_id IS NOT NULL
    AND TRIM(coib.product_id) <> ''
    AND (coib.quantity IS NULL OR CAST(coib.quantity AS INT) >= 0)
    AND (coib.line_amount IS NULL OR CAST(coib.line_amount AS DECIMAL(18,2)) >= 0)
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id
      ORDER BY
        CAST(line_amount AS DECIMAL(18,2)) DESC NULLS LAST,
        CAST(quantity AS INT) DESC NULLS LAST,
        product_name DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  CONCAT(coib.order_id, '-', coib.product_id)                                                   AS order_item_id,
  coib.order_id                                                                                AS order_id,
  coib.product_id                                                                              AS product_id,
  CAST(coib.quantity AS INT)                                                                    AS quantity,
  CAST(coib.line_amount AS DECIMAL(18,2))                                                       AS line_amount,
  CASE
    WHEN coib.quantity IS NOT NULL AND CAST(coib.quantity AS INT) <> 0
      THEN CAST(coib.line_amount AS DECIMAL(18,2)) / CAST(coib.quantity AS INT)
    ELSE 0
  END                                                                                           AS unit_price,
  'USD'                                                                                         AS currency_code
FROM dedup coib
WHERE coib.rn = 1
"""
customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

# ======================================================================================
# Target: silver.product_silver
# - De-duplicate by product_id keeping latest record (deterministic fallback ordering)
# - Filter invalid: product_id null/blank
# ======================================================================================
product_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id,
    pb.product_name,
    pb.category,
    pb.is_active
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        is_active DESC NULLS LAST,
        product_name DESC NULLS LAST,
        category DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  pb.product_id                                              AS product_id,
  TRIM(pb.product_name)                                      AS product_name,
  TRIM(pb.category)                                          AS product_category,
  CASE WHEN pb.is_active = TRUE THEN 'ACTIVE' ELSE 'INACTIVE' END AS product_status
FROM dedup pb
WHERE pb.rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ======================================================================================
# Target: silver.customer_silver
# - Customer scaffold from customer_orders_bronze
# - De-duplicate by store_id
# - customer_created_date = MIN(CAST(order_time AS DATE)) OVER (PARTITION BY store_id)
# - Filter invalid: store_id null/blank
# ======================================================================================
customer_silver_sql = """
WITH base AS (
  SELECT
    cob.store_id,
    cob.order_time
  FROM customer_orders_bronze cob
  WHERE cob.store_id IS NOT NULL
    AND TRIM(cob.store_id) <> ''
),
enriched AS (
  SELECT
    cob.store_id                                                             AS customer_id,
    CONCAT('Store ', cob.store_id)                                            AS customer_name,
    CAST(NULL AS STRING)                                                     AS customer_email,
    MIN(CAST(cob.order_time AS DATE)) OVER (PARTITION BY cob.store_id)        AS customer_created_date,
    'ACTIVE'                                                                 AS customer_status
  FROM base cob
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY customer_created_date ASC
    ) AS rn
  FROM enriched
)
SELECT
  customer_id,
  customer_name,
  customer_email,
  customer_created_date,
  customer_status
FROM dedup
WHERE rn = 1
"""
customer_silver_df = spark.sql(customer_silver_sql)

(
    customer_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_silver.csv")
)

# ======================================================================================
# Target: silver.etl_load_audit_orders_silver
# - Group by data_date, source_system_table
# - Ensure one row per (data_date, source_system_table)
# ======================================================================================
etl_load_audit_orders_silver_sql = """
WITH agg AS (
  SELECT
    CAST(elaob.load_watermark_time AS DATE)                                  AS data_date,
    elaob.source_system_table                                               AS source_system_table,
    COUNT(DISTINCT elaob.reference_key)                                      AS records_received,
    COUNT(DISTINCT elaob.reference_key)                                      AS records_loaded,
    MIN(elaob.load_watermark_time)                                           AS load_start_ts,
    MAX(elaob.load_watermark_time)                                           AS load_end_ts
  FROM etl_load_audit_orders_bronze elaob
  GROUP BY
    CAST(elaob.load_watermark_time AS DATE),
    elaob.source_system_table
)
SELECT
  CONCAT(source_system_table, '-', DATE_FORMAT(data_date, 'yyyyMMdd'))       AS load_id,
  data_date                                                                 AS data_date,
  CAST(NULL AS STRING)                                                      AS source_bucket,
  CAST(NULL AS STRING)                                                      AS source_object_prefix,
  CAST(records_received AS BIGINT)                                           AS records_received,
  CAST(records_loaded AS BIGINT)                                             AS records_loaded,
  CAST(0 AS BIGINT)                                                         AS records_rejected,
  load_start_ts                                                             AS load_start_ts,
  load_end_ts                                                               AS load_end_ts,
  'SUCCESS'                                                                 AS load_status
FROM agg
"""
etl_load_audit_orders_silver_df = spark.sql(etl_load_audit_orders_silver_sql)

(
    etl_load_audit_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_load_audit_orders_silver.csv")
)

job.commit()
```