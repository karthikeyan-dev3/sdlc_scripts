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
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read SOURCE tables from S3 (Bronze) and create temp views
# NOTE: Per requirement, ALWAYS use: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
bronze_customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
bronze_customer_orders_df.createOrReplaceTempView("bronze_customer_orders")

bronze_customer_order_items_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
bronze_customer_order_items_df.createOrReplaceTempView("bronze_customer_order_items")

bronze_etl_load_audit_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_load_audit.{FILE_FORMAT}/")
)
bronze_etl_load_audit_df.createOrReplaceTempView("bronze_etl_load_audit")

# ===================================================================================
# Target: silver.etl_load_audit
# 1) Read source: bronze.etl_load_audit
# 2) Temp view already created: bronze_etl_load_audit
# 3) Transform with Spark SQL (CAST/COALESCE/TRIM/UPPER/LOWER/DATE + ROW_NUMBER)
# 4) Write output as SINGLE CSV file: TARGET_PATH + "/etl_load_audit.csv"
# ===================================================================================
etl_load_audit_sql = """
WITH dedup AS (
  SELECT
    -- UDT mappings
    CAST(TRIM(ela.run_id) AS STRING)                                              AS pipeline_run_id,
    CAST(TRIM(ela.source_name) AS STRING)                                         AS dataset_name,
    CAST(ela.load_timestamp AS DATE)                                              AS ingestion_date,
    CAST('sales_transactions_raw' AS STRING)                                      AS source_system,
    CAST(ela.record_count AS BIGINT)                                              AS records_received,
    CAST(ela.record_count AS BIGINT)                                              AS records_loaded,
    CAST(0 AS BIGINT)                                                             AS records_rejected,
    CAST(TRIM(ela.status) AS STRING)                                              AS validation_status,
    CAST(ela.load_timestamp AS TIMESTAMP)                                         AS processing_start_ts,
    CAST(ela.load_timestamp AS TIMESTAMP)                                         AS processing_end_ts,

    -- Dedup controls (required: Deduplicate by (run_id, source_name, load_timestamp) keeping latest load_timestamp)
    ROW_NUMBER() OVER (
      PARTITION BY ela.run_id, ela.source_name, ela.load_timestamp
      ORDER BY ela.load_timestamp DESC
    ) AS rn
  FROM bronze_etl_load_audit ela
)
SELECT
  pipeline_run_id,
  dataset_name,
  ingestion_date,
  source_system,
  records_received,
  records_loaded,
  records_rejected,
  validation_status,
  processing_start_ts,
  processing_end_ts
FROM dedup
WHERE rn = 1
"""

silver_etl_load_audit_df = spark.sql(etl_load_audit_sql)
silver_etl_load_audit_df.createOrReplaceTempView("silver_etl_load_audit")

(
    silver_etl_load_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_load_audit.csv")
)

# ===================================================================================
# Target: silver.customer_orders
# 1) Read source: bronze.customer_orders + bronze.etl_load_audit
# 2) Temp views already created: bronze_customer_orders, bronze_etl_load_audit
# 3) Transform with Spark SQL (CAST/COALESCE/TRIM/UPPER/LOWER/DATE + ROW_NUMBER)
# 4) Write output as SINGLE CSV file: TARGET_PATH + "/customer_orders.csv"
# ===================================================================================
customer_orders_sql = """
WITH joined AS (
  SELECT
    co.*,
    ela.load_timestamp AS ela_load_timestamp,
    ela.ingestion_date AS ela_ingestion_date
  FROM bronze_customer_orders co
  LEFT JOIN bronze_etl_load_audit ela
    ON ela.source_name = 'sales_transactions_raw'
   AND ela.batch_id = co.batch_id
),
ranked AS (
  SELECT
    -- UDT mappings / conforming columns
    CAST(TRIM(co.order_id) AS STRING)                                             AS order_id,
    CAST(co.order_time AS DATE)                                                   AS order_date,
    CAST(NULL AS STRING)                                                          AS customer_id,
    CAST(NULL AS STRING)                                                          AS order_status,
    CAST(co.order_total_amount AS DECIMAL(18, 2))                                 AS order_total_amount,
    CAST(NULL AS STRING)                                                          AS currency_code,

    -- ingestion_date: per column mapping -> CAST(ela.load_timestamp AS DATE)
    -- plus description allows fallback; implement COALESCE with CAST(co.order_time AS DATE)
    CAST(COALESCE(CAST(ela_load_timestamp AS DATE), CAST(co.order_time AS DATE)) AS DATE) AS ingestion_date,

    -- source_file_path: per mapping -> NULL (description mentions co.source_file_path if present)
    CAST(NULL AS STRING)                                                          AS source_file_path,

    -- load_timestamp: per mapping -> ela.load_timestamp (description allows CURRENT_TIMESTAMP fallback)
    CAST(COALESCE(ela_load_timestamp, CURRENT_TIMESTAMP()) AS TIMESTAMP)          AS load_timestamp,

    -- De-dup: on order_id (keep latest by order_time, then load_timestamp)
    ROW_NUMBER() OVER (
      PARTITION BY co.order_id
      ORDER BY co.order_time DESC, COALESCE(ela_load_timestamp, CURRENT_TIMESTAMP()) DESC
    ) AS rn
  FROM joined co
)
SELECT DISTINCT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_total_amount,
  currency_code,
  ingestion_date,
  source_file_path,
  load_timestamp
FROM ranked
WHERE rn = 1
"""

silver_customer_orders_df = spark.sql(customer_orders_sql)
silver_customer_orders_df.createOrReplaceTempView("silver_customer_orders")

(
    silver_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# Target: silver.customer_order_items
# 1) Read source: bronze.customer_order_items + silver.customer_orders (temp view)
# 2) Temp views already created: bronze_customer_order_items, silver_customer_orders
# 3) Transform with Spark SQL (CAST/COALESCE/TRIM/UPPER/LOWER/DATE + ROW_NUMBER)
# 4) Write output as SINGLE CSV file: TARGET_PATH + "/customer_order_items.csv"
# ===================================================================================
customer_order_items_sql = """
WITH base AS (
  SELECT
    coi.*
  FROM bronze_customer_order_items coi
  INNER JOIN silver_customer_orders sco
    ON sco.order_id = coi.order_id
),
dedup AS (
  SELECT
    CAST(TRIM(coi.order_id) AS STRING)                                            AS order_id,

    CAST(
      ROW_NUMBER() OVER (
        PARTITION BY coi.order_id
        ORDER BY coi.product_id, coi.quantity, coi.line_amount
      ) AS BIGINT
    ) AS order_item_id,

    CAST(TRIM(coi.product_id) AS STRING)                                          AS product_id,
    CAST(coi.quantity AS INT)                                                     AS quantity,

    CAST(
      CASE
        WHEN coi.quantity <> 0 THEN (CAST(coi.line_amount AS DECIMAL(18, 6)) / CAST(coi.quantity AS DECIMAL(18, 6)))
        ELSE NULL
      END
      AS DECIMAL(18, 6)
    ) AS unit_price,

    CAST(coi.line_amount AS DECIMAL(18, 2))                                       AS item_total_amount
  FROM base coi
)
SELECT DISTINCT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  item_total_amount
FROM dedup
"""

silver_customer_order_items_df = spark.sql(customer_order_items_sql)
silver_customer_order_items_df.createOrReplaceTempView("silver_customer_order_items")

(
    silver_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items.csv")
)

job.commit()
```