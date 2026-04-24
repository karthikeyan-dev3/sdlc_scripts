```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Job Setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Spark CSV Read Options
# ------------------------------------------------------------------------------------
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# ====================================================================================
# SOURCE: silver.customer_orders (sco)
# ====================================================================================
sco_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.csv/")
)
sco_df.createOrReplaceTempView("customer_orders")

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_customer_orders (gco)
# - Direct mapping from sco to gco
# - Dedup enforced to one row per order_id via ROW_NUMBER
# ------------------------------------------------------------------------------------
gco_sql = """
WITH ranked AS (
  SELECT
    CAST(sco.order_id AS STRING)                    AS order_id,
    CAST(sco.order_date AS DATE)                    AS order_date,
    CAST(sco.order_status AS STRING)                AS order_status,
    CAST(sco.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
    CAST(sco.currency_code AS STRING)               AS currency_code,
    CAST(sco.source_system AS STRING)               AS source_system,
    CAST(sco.ingestion_date AS DATE)                AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sco.order_id AS STRING)
      ORDER BY CAST(sco.ingestion_date AS DATE) DESC
    ) AS rn
  FROM customer_orders sco
)
SELECT
  order_id,
  order_date,
  order_status,
  order_total_amount,
  currency_code,
  source_system,
  ingestion_date
FROM ranked
WHERE rn = 1
"""
gco_df = spark.sql(gco_sql)

# Write SINGLE CSV file directly under TARGET_PATH
(
    gco_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ====================================================================================
# SOURCE: silver.customer_order_items (scoi)
# ====================================================================================
scoi_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items.csv/")
)
scoi_df.createOrReplaceTempView("customer_order_items")

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_customer_order_items (gcoi)
# - Direct mapping from scoi to gcoi
# - Dedup enforced to one row per (order_id, order_item_id) via ROW_NUMBER
# ------------------------------------------------------------------------------------
gcoi_sql = """
WITH ranked AS (
  SELECT
    CAST(scoi.order_id AS STRING)                    AS order_id,
    CAST(scoi.order_item_id AS STRING)               AS order_item_id,
    CAST(scoi.product_id AS STRING)                  AS product_id,
    CAST(scoi.quantity AS INT)                       AS quantity,
    CAST(scoi.unit_price AS DECIMAL(38, 10))         AS unit_price,
    CAST(scoi.line_amount AS DECIMAL(38, 10))        AS line_amount,
    CAST(scoi.currency_code AS STRING)               AS currency_code,
    CAST(scoi.ingestion_date AS DATE)                AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY
        CAST(scoi.order_id AS STRING),
        CAST(scoi.order_item_id AS STRING)
      ORDER BY CAST(scoi.ingestion_date AS DATE) DESC
    ) AS rn
  FROM customer_order_items scoi
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount,
  currency_code,
  ingestion_date
FROM ranked
WHERE rn = 1
"""
gcoi_df = spark.sql(gcoi_sql)

# Write SINGLE CSV file directly under TARGET_PATH
(
    gcoi_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ====================================================================================
# SOURCE: silver.ingestion_run_audit (sira)
# ====================================================================================
sira_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/ingestion_run_audit.csv/")
)
sira_df.createOrReplaceTempView("ingestion_run_audit")

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_ingestion_run_audit (gira)
# - Direct mapping from sira to gira
# - Dedup enforced to one row per run_id via ROW_NUMBER
# ------------------------------------------------------------------------------------
gira_sql = """
WITH ranked AS (
  SELECT
    CAST(sira.run_id AS STRING)              AS run_id,
    CAST(sira.source_file_path AS STRING)    AS source_file_path,
    CAST(sira.source_file_format AS STRING)  AS source_file_format,
    CAST(sira.ingestion_date AS DATE)        AS ingestion_date,
    CAST(sira.run_start_ts AS TIMESTAMP)     AS run_start_ts,
    CAST(sira.run_end_ts AS TIMESTAMP)       AS run_end_ts,
    CAST(sira.run_status AS STRING)          AS run_status,
    CAST(sira.records_read AS BIGINT)        AS records_read,
    CAST(sira.records_loaded AS BIGINT)      AS records_loaded,
    CAST(sira.records_rejected AS BIGINT)    AS records_rejected,
    CAST(sira.error_reason AS STRING)        AS error_reason,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sira.run_id AS STRING)
      ORDER BY CAST(sira.run_start_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM ingestion_run_audit sira
)
SELECT
  run_id,
  source_file_path,
  source_file_format,
  ingestion_date,
  run_start_ts,
  run_end_ts,
  run_status,
  records_read,
  records_loaded,
  records_rejected,
  error_reason
FROM ranked
WHERE rn = 1
"""
gira_df = spark.sql(gira_sql)

# Write SINGLE CSV file directly under TARGET_PATH
(
    gira_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_ingestion_run_audit.csv")
)

job.commit()
```