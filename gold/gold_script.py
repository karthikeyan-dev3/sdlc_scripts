```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------------------------
# AWS Glue job bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV read options (kept explicit and consistent)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\""
}

# Ensure Spark writes a single output file by repartitioning to 1
# (Note: Spark will still create a folder at the target path. This script writes to the exact requested path.)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
#    SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.csv/")
)
customer_orders_df.createOrReplaceTempView("customer_orders")

order_items_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/order_items.csv/")
)
order_items_df.createOrReplaceTempView("order_items")

etl_run_log_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/etl_run_log.csv/")
)
etl_run_log_df.createOrReplaceTempView("etl_run_log")

data_quality_results_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/data_quality_results.csv/")
)
data_quality_results_df.createOrReplaceTempView("data_quality_results")

# ------------------------------------------------------------------------------------
# 2) Transform + 3) Write each target table separately
# ------------------------------------------------------------------------------------

# ============================================================
# Target: gold.gold_customer_orders
# Source: silver.customer_orders sco
# ============================================================
gold_customer_orders_sql = """
SELECT
  CAST(sco.order_id AS STRING)              AS order_id,
  CAST(sco.order_date AS DATE)              AS order_date,
  CAST(sco.customer_id AS STRING)           AS customer_id,
  CAST(sco.order_status AS STRING)          AS order_status,
  CAST(sco.currency_code AS STRING)         AS currency_code,
  CAST(sco.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
  CAST(sco.source_file_date AS DATE)        AS source_file_date,
  CAST(sco.loaded_at AS TIMESTAMP)          AS loaded_at
FROM customer_orders sco
"""
gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ============================================================
# Target: gold.gold_order_items
# Source: silver.order_items soi
# ============================================================
gold_order_items_sql = """
SELECT
  CAST(soi.order_id AS STRING)              AS order_id,
  CAST(soi.order_item_id AS INT)            AS order_item_id,
  CAST(soi.product_id AS STRING)            AS product_id,
  CAST(soi.quantity AS INT)                 AS quantity,
  CAST(soi.unit_price_amount AS DECIMAL(38, 10)) AS unit_price_amount,
  CAST(soi.line_total_amount AS DECIMAL(38, 10)) AS line_total_amount
FROM order_items soi
"""
gold_order_items_df = spark.sql(gold_order_items_sql)

(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# ============================================================
# Target: gold.gold_etl_run_log
# Source: silver.etl_run_log serl
# Note: Apply ROW_NUMBER for dedup (latest per etl_run_id)
# ============================================================
gold_etl_run_log_sql = """
WITH ranked AS (
  SELECT
    CAST(serl.etl_run_id AS STRING)          AS etl_run_id,
    CAST(serl.pipeline_name AS STRING)       AS pipeline_name,
    CAST(serl.run_date AS DATE)              AS run_date,
    CAST(serl.start_ts AS TIMESTAMP)         AS start_ts,
    CAST(serl.end_ts AS TIMESTAMP)           AS end_ts,
    CAST(serl.status AS STRING)              AS status,
    CAST(serl.records_received AS INT)       AS records_received,
    CAST(serl.records_valid AS INT)          AS records_valid,
    CAST(serl.records_rejected AS INT)       AS records_rejected,
    CAST(serl.error_message AS STRING)       AS error_message,
    ROW_NUMBER() OVER (
      PARTITION BY serl.etl_run_id
      ORDER BY CAST(serl.start_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM etl_run_log serl
)
SELECT
  etl_run_id,
  pipeline_name,
  run_date,
  start_ts,
  end_ts,
  status,
  records_received,
  records_valid,
  records_rejected,
  error_message
FROM ranked
WHERE rn = 1
"""
gold_etl_run_log_df = spark.sql(gold_etl_run_log_sql)

(
    gold_etl_run_log_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_run_log.csv")
)

# ============================================================
# Target: gold.gold_data_quality_results
# Source: silver.data_quality_results sdqr
# ============================================================
gold_data_quality_results_sql = """
SELECT
  CAST(sdqr.etl_run_id AS STRING)            AS etl_run_id,
  CAST(sdqr.rule_name AS STRING)             AS rule_name,
  CAST(sdqr.rule_description AS STRING)      AS rule_description,
  CAST(sdqr.severity AS STRING)              AS severity,
  CAST(sdqr.failed_record_count AS INT)      AS failed_record_count
FROM data_quality_results sdqr
"""
gold_data_quality_results_df = spark.sql(gold_data_quality_results_sql)

(
    gold_data_quality_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_results.csv")
)

job.commit()
```