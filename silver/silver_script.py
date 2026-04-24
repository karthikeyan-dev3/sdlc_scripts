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
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV options (kept simple and explicit)
read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": '"',
}

# Ensure Spark writes a single file (we will still rename the part file to the required .csv name)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------------------------------------------------------------
# Source reads (S3) + temp views
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items")

ingestion_audit_daily_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/ingestion_audit_daily.{FILE_FORMAT}/")
)
ingestion_audit_daily_bronze_df.createOrReplaceTempView("ingestion_audit_daily")

# -----------------------------------------------------------------------------------
# TARGET: silver.customer_orders
# Grain: order
# Dedup: by order_id keeping latest order_time
# Transformations: EXACT per UDT
# -----------------------------------------------------------------------------------
customer_orders_sql = """
WITH ranked AS (
  SELECT
    co.order_id AS order_id,
    CAST(co.order_time AS DATE) AS order_date,
    CAST(NULL AS STRING) AS customer_id,
    CAST(NULL AS STRING) AS order_status,
    co.order_total_amount AS order_total_amount,
    CAST(NULL AS STRING) AS currency_code,
    'sales_transactions_raw' AS source_system,
    CURRENT_DATE() AS ingestion_date,
    co.order_time AS _order_time_ts,
    ROW_NUMBER() OVER (PARTITION BY co.order_id ORDER BY co.order_time DESC) AS rn
  FROM customer_orders co
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_total_amount,
  currency_code,
  source_system,
  ingestion_date,
  SHA2(
    CONCAT_WS('||',
      order_id,
      order_date,
      customer_id,
      order_status,
      order_total_amount,
      currency_code,
      source_system
    ),
    256
  ) AS record_hash
FROM ranked
WHERE rn = 1
"""
silver_customer_orders_df = spark.sql(customer_orders_sql)

# Write as SINGLE CSV file directly under TARGET_PATH as: TARGET_PATH + "/customer_orders.csv"
# Note: Spark writes a part file; we write to a temp folder then rename to the required single file.
customer_orders_tmp_path = f"{TARGET_PATH}/__tmp_customer_orders_csv/"
(
    silver_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(customer_orders_tmp_path)
)

# Rename part file to exact required name (customer_orders.csv) directly under TARGET_PATH
customer_orders_part = [
    f.path for f in dbutils.fs.ls(customer_orders_tmp_path) if f.path.endswith(".csv")
][0]
dbutils.fs.cp(customer_orders_part, f"{TARGET_PATH}/customer_orders.csv", True)
dbutils.fs.rm(customer_orders_tmp_path, True)

# -----------------------------------------------------------------------------------
# TARGET: silver.customer_order_items
# Grain: order line
# Dedup: by (order_id, product_id) keeping latest co.order_time
# Transformations: EXACT per UDT
# -----------------------------------------------------------------------------------
customer_order_items_sql = """
WITH ranked AS (
  SELECT
    coi.order_id AS order_id,
    SHA2(CONCAT_WS('||', coi.order_id, coi.product_id), 256) AS order_item_id,
    coi.product_id AS product_id,
    coi.quantity AS quantity,
    CASE WHEN coi.quantity <> 0 THEN coi.line_amount / coi.quantity ELSE NULL END AS unit_price_amount,
    coi.line_amount AS line_total_amount,
    CAST(NULL AS STRING) AS currency_code,
    'sales_transactions_raw' AS source_system,
    CURRENT_DATE() AS ingestion_date,
    co.order_time AS _order_time_ts,
    ROW_NUMBER() OVER (
      PARTITION BY coi.order_id, coi.product_id
      ORDER BY co.order_time DESC
    ) AS rn
  FROM customer_order_items coi
  INNER JOIN customer_orders co
    ON coi.order_id = co.order_id
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price_amount,
  line_total_amount,
  currency_code,
  source_system,
  ingestion_date,
  SHA2(
    CONCAT_WS('||',
      order_id,
      order_item_id,
      product_id,
      quantity,
      unit_price_amount,
      line_total_amount,
      currency_code,
      source_system
    ),
    256
  ) AS record_hash
FROM ranked
WHERE rn = 1
"""
silver_customer_order_items_df = spark.sql(customer_order_items_sql)

# Write as SINGLE CSV file directly under TARGET_PATH as: TARGET_PATH + "/customer_order_items.csv"
customer_order_items_tmp_path = f"{TARGET_PATH}/__tmp_customer_order_items_csv/"
(
    silver_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(customer_order_items_tmp_path)
)

customer_order_items_part = [
    f.path for f in dbutils.fs.ls(customer_order_items_tmp_path) if f.path.endswith(".csv")
][0]
dbutils.fs.cp(customer_order_items_part, f"{TARGET_PATH}/customer_order_items.csv", True)
dbutils.fs.rm(customer_order_items_tmp_path, True)

# -----------------------------------------------------------------------------------
# TARGET: silver.ingestion_audit_daily
# Dedup: by (ingestion_date, source_system, dataset_name) keeping latest load_timestamp
# Transformations: EXACT per UDT
# -----------------------------------------------------------------------------------
ingestion_audit_daily_sql = """
WITH ranked AS (
  SELECT
    iad.ingestion_date AS ingestion_date,
    'sales_transactions_raw' AS source_system,
    iad.source_table_name AS dataset_name,
    CAST(NULL AS BIGINT) AS s3_object_count,
    iad.record_count AS record_count_ingested,
    CAST(0 AS BIGINT) AS record_count_rejected,
    'UNKNOWN' AS validation_status,
    CAST(NULL AS STRING) AS pipeline_run_id,
    iad.load_timestamp AS run_start_ts,
    iad.load_timestamp AS run_end_ts,
    iad.load_timestamp AS _load_ts,
    ROW_NUMBER() OVER (
      PARTITION BY iad.ingestion_date, 'sales_transactions_raw', iad.source_table_name
      ORDER BY iad.load_timestamp DESC
    ) AS rn
  FROM ingestion_audit_daily iad
)
SELECT
  ingestion_date,
  source_system,
  dataset_name,
  s3_object_count,
  record_count_ingested,
  record_count_rejected,
  validation_status,
  pipeline_run_id,
  run_start_ts,
  run_end_ts
FROM ranked
WHERE rn = 1
"""
silver_ingestion_audit_daily_df = spark.sql(ingestion_audit_daily_sql)

# Write as SINGLE CSV file directly under TARGET_PATH as: TARGET_PATH + "/ingestion_audit_daily.csv"
ingestion_audit_daily_tmp_path = f"{TARGET_PATH}/__tmp_ingestion_audit_daily_csv/"
(
    silver_ingestion_audit_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(ingestion_audit_daily_tmp_path)
)

ingestion_audit_daily_part = [
    f.path for f in dbutils.fs.ls(ingestion_audit_daily_tmp_path) if f.path.endswith(".csv")
][0]
dbutils.fs.cp(ingestion_audit_daily_part, f"{TARGET_PATH}/ingestion_audit_daily.csv", True)
dbutils.fs.rm(ingestion_audit_daily_tmp_path, True)

job.commit()
```