```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Common CSV options (adjust if your bronze files differ)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# ===================================================================================
# TABLE: silver.customer_orders
# ===================================================================================

# 1) Read source table(s) from S3 (STRICT SOURCE READING RULE)
customer_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
customer_orders_src_df.createOrReplaceTempView("customer_orders")  # alias: co in SQL

# 3) Transform using Spark SQL (includes dedup with ROW_NUMBER)
customer_orders_sql = """
WITH base AS (
  SELECT
    co.order_id                                                                 AS order_id,
    CAST(co.order_timestamp AS DATE)                                            AS order_date,
    CASE WHEN co.order_total_amount >= 0 THEN co.order_total_amount ELSE NULL END AS order_total_amount,
    'UNKNOWN'                                                                   AS order_status,
    'USD'                                                                       AS currency_code,
    'sales_transactions_raw'                                                    AS source_system,
    CAST(CURRENT_DATE AS DATE)                                                  AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY co.order_id
      ORDER BY co.order_timestamp DESC
    ) AS rn
  FROM customer_orders co
)
SELECT
  order_id,
  order_date,
  order_total_amount,
  order_status,
  currency_code,
  source_system,
  ingestion_date
FROM base
WHERE rn = 1
"""

customer_orders_tgt_df = spark.sql(customer_orders_sql)

# 4) Write output (SINGLE CSV file directly under TARGET_PATH)
(
    customer_orders_tgt_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TABLE: silver.customer_order_items
# ===================================================================================

# 1) Read source table(s) from S3 (STRICT SOURCE READING RULE)
customer_order_items_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
customer_order_items_src_df.createOrReplaceTempView("customer_order_items")  # alias: coi in SQL

# 3) Transform using Spark SQL (includes dedup with ROW_NUMBER)
customer_order_items_sql = """
WITH base AS (
  SELECT
    coi.order_id                                                   AS order_id,
    CAST(xxhash64(coi.order_id, coi.product_id) AS STRING)         AS order_item_id,
    coi.product_id                                                 AS product_id,
    CASE WHEN coi.quantity > 0 THEN coi.quantity ELSE NULL END     AS quantity,
    CASE
      WHEN coi.quantity > 0 THEN coi.line_amount / COALESCE(coi.quantity, 0)
      ELSE NULL
    END                                                            AS unit_price,
    CASE WHEN coi.line_amount >= 0 THEN coi.line_amount ELSE NULL END AS line_amount,
    'USD'                                                          AS currency_code,
    CAST(CURRENT_DATE AS DATE)                                     AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY coi.order_id, CAST(xxhash64(coi.order_id, coi.product_id) AS STRING)
      ORDER BY coi.product_id ASC
    ) AS rn
  FROM customer_order_items coi
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
FROM base
WHERE rn = 1
"""

customer_order_items_tgt_df = spark.sql(customer_order_items_sql)

# 4) Write output (SINGLE CSV file directly under TARGET_PATH)
(
    customer_order_items_tgt_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items.csv")
)

# ===================================================================================
# TABLE: silver.ingestion_run_audit
# ===================================================================================

# 1) Read source table(s) from S3 (STRICT SOURCE READING RULE)
ingestion_run_audit_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/ingestion_run_audit.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
ingestion_run_audit_src_df.createOrReplaceTempView("ingestion_run_audit")  # alias: ira in SQL

# 3) Transform using Spark SQL (includes dedup with ROW_NUMBER)
ingestion_run_audit_sql = """
WITH base AS (
  SELECT
    ira.run_id                                                     AS run_id,
    ira.source_name                                                AS source_file_path,
    'UNKNOWN'                                                      AS source_file_format,
    CAST(ira.ingestion_start_timestamp AS DATE)                    AS ingestion_date,
    ira.ingestion_start_timestamp                                  AS run_start_ts,
    ira.ingestion_end_timestamp                                    AS run_end_ts,
    ira.load_status                                                AS run_status,
    ira.record_count                                               AS records_read,
    CAST(NULL AS BIGINT)                                           AS records_loaded,
    CAST(NULL AS BIGINT)                                           AS records_rejected,
    CASE
      WHEN ira.load_status IN ('FAILED', 'ERROR') THEN 'UNKNOWN'
      ELSE NULL
    END                                                            AS error_reason,
    ROW_NUMBER() OVER (
      PARTITION BY ira.run_id, ira.source_name
      ORDER BY ira.ingestion_start_timestamp DESC
    ) AS rn
  FROM ingestion_run_audit ira
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
FROM base
WHERE rn = 1
"""

ingestion_run_audit_tgt_df = spark.sql(ingestion_run_audit_sql)

# 4) Write output (SINGLE CSV file directly under TARGET_PATH)
(
    ingestion_run_audit_tgt_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestion_run_audit.csv")
)

job.commit()
```