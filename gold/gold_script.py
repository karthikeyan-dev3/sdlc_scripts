```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue job setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV options (safe defaults for typical silver CSV outputs)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\""
}

# ====================================================================================
# TABLE: gold.gold_customer_orders  (source: silver.customer_orders sco)
# ====================================================================================

# 1) Read source tables from S3
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

# 2) Create temp views
customer_orders_silver_df.createOrReplaceTempView("customer_orders")

# 3) SQL transformation (exact mappings)
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(sco.order_id AS STRING)                         AS order_id,
  CAST(sco.order_date AS DATE)                         AS order_date,
  CAST(sco.customer_id AS STRING)                      AS customer_id,
  CAST(sco.order_status AS STRING)                     AS order_status,
  CAST(sco.order_total_amount AS DECIMAL(38,10))       AS order_total_amount,
  CAST(sco.currency_code AS STRING)                    AS currency_code,
  CAST(sco.source_system AS STRING)                    AS source_system,
  CAST(sco.ingestion_date AS DATE)                     AS ingestion_date,
  CAST(sco.record_hash AS STRING)                      AS record_hash
FROM customer_orders sco
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ====================================================================================
# TABLE: gold.gold_customer_order_items  (source: silver.customer_order_items scoi)
# ====================================================================================

# 1) Read source tables from S3
customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

# 2) Create temp views
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items")

# 3) SQL transformation (exact mappings)
gold_customer_order_items_df = spark.sql("""
SELECT
  CAST(scoi.order_id AS STRING)                    AS order_id,
  CAST(scoi.order_item_id AS STRING)               AS order_item_id,
  CAST(scoi.product_id AS STRING)                  AS product_id,
  CAST(scoi.quantity AS INT)                       AS quantity,
  CAST(scoi.unit_price_amount AS DECIMAL(38,10))   AS unit_price_amount,
  CAST(scoi.line_total_amount AS DECIMAL(38,10))   AS line_total_amount,
  CAST(scoi.currency_code AS STRING)               AS currency_code,
  CAST(scoi.source_system AS STRING)               AS source_system,
  CAST(scoi.ingestion_date AS DATE)                AS ingestion_date,
  CAST(scoi.record_hash AS STRING)                 AS record_hash
FROM customer_order_items scoi
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ====================================================================================
# TABLE: gold.gold_ingestion_audit_daily  (source: silver.ingestion_audit_daily siad)
# ====================================================================================

# 1) Read source tables from S3
ingestion_audit_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/ingestion_audit_daily.{FILE_FORMAT}/")
)

# 2) Create temp views
ingestion_audit_daily_silver_df.createOrReplaceTempView("ingestion_audit_daily")

# 3) SQL transformation (exact mappings)
gold_ingestion_audit_daily_df = spark.sql("""
SELECT
  CAST(siad.ingestion_date AS DATE)            AS ingestion_date,
  CAST(siad.source_system AS STRING)           AS source_system,
  CAST(siad.dataset_name AS STRING)            AS dataset_name,
  CAST(siad.s3_object_count AS BIGINT)         AS s3_object_count,
  CAST(siad.record_count_ingested AS BIGINT)   AS record_count_ingested,
  CAST(siad.record_count_rejected AS BIGINT)   AS record_count_rejected,
  CAST(siad.validation_status AS STRING)       AS validation_status,
  CAST(siad.pipeline_run_id AS STRING)         AS pipeline_run_id,
  CAST(siad.run_start_ts AS TIMESTAMP)         AS run_start_ts,
  CAST(siad.run_end_ts AS TIMESTAMP)           AS run_end_ts
FROM ingestion_audit_daily siad
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    gold_ingestion_audit_daily_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_ingestion_audit_daily.csv")
)

job.commit()
```