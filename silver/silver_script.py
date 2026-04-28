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
# 1) READ SOURCE TABLES (S3) + TEMP VIEWS
#    NOTE: Using required path pattern: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items")

etl_run_log_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_run_log.{FILE_FORMAT}/")
)
etl_run_log_bronze_df.createOrReplaceTempView("etl_run_log")

data_quality_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_results.{FILE_FORMAT}/")
)
data_quality_results_bronze_df.createOrReplaceTempView("data_quality_results")

# -----------------------------------------------------------------------------------
# TARGET TABLE: silver.customer_orders
# - Transformations (EXACT from UDT):
#   order_id = co.transaction_id
#   order_date = DATE(co.transaction_time)
#   customer_id = co.store_id
#   order_status = 'completed'
#   currency_code = 'USD'
#   order_total_amount = SUM(co.sale_amount) OVER (PARTITION BY co.transaction_id)
#   source_file_date = DATE(co.transaction_time)
#   loaded_at = CURRENT_TIMESTAMP
# - Dedup rule: keep latest record per transaction_id using max(transaction_time)
# -----------------------------------------------------------------------------------
customer_orders_sql = """
WITH base AS (
  SELECT
      co.transaction_id,
      co.transaction_time,
      co.store_id,
      co.sale_amount,
      ROW_NUMBER() OVER (
        PARTITION BY co.transaction_id
        ORDER BY co.transaction_time DESC
      ) AS rn_latest,
      SUM(co.sale_amount) OVER (PARTITION BY co.transaction_id) AS order_total_amount
  FROM customer_orders co
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn_latest = 1
)
SELECT
  CAST(transaction_id AS STRING)                                 AS order_id,
  DATE(transaction_time)                                         AS order_date,
  CAST(store_id AS STRING)                                       AS customer_id,
  CAST('completed' AS STRING)                                    AS order_status,
  CAST('USD' AS STRING)                                          AS currency_code,
  CAST(order_total_amount AS DECIMAL(38, 10))                    AS order_total_amount,
  DATE(transaction_time)                                         AS source_file_date,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                           AS loaded_at
FROM dedup
"""

customer_orders_silver_df = spark.sql(customer_orders_sql)

# WRITE: single CSV directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: silver.order_items
# - Transformations (EXACT from UDT):
#   order_id = oi.transaction_id
#   order_item_id = ROW_NUMBER() OVER (PARTITION BY oi.transaction_id ORDER BY oi.product_id, oi.transaction_time, oi.store_id)
#   product_id = oi.product_id
#   quantity = CASE WHEN COALESCE(oi.quantity,1) < 0 THEN 0 ELSE COALESCE(oi.quantity,1) END
#   unit_price_amount = CASE WHEN COALESCE(oi.quantity,0) > 0 THEN oi.sale_amount/oi.quantity ELSE NULL END
#   line_total_amount = oi.sale_amount
# - Dedup rule: remove exact duplicates on (transaction_id, product_id, quantity, sale_amount, transaction_time, store_id)
# -----------------------------------------------------------------------------------
order_items_sql = """
WITH typed AS (
  SELECT
    CAST(oi.transaction_id AS STRING)         AS transaction_id,
    CAST(oi.product_id AS STRING)             AS product_id,
    CAST(oi.transaction_time AS TIMESTAMP)    AS transaction_time,
    CAST(oi.store_id AS STRING)               AS store_id,
    CAST(oi.quantity AS INT)                  AS quantity,
    CAST(oi.sale_amount AS DECIMAL(38, 10))   AS sale_amount
  FROM order_items oi
),
dedup AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.transaction_id,
        t.product_id,
        t.quantity,
        t.sale_amount,
        t.transaction_time,
        t.store_id
      ORDER BY t.transaction_time DESC
    ) AS rn_dedup
  FROM typed t
),
kept AS (
  SELECT *
  FROM dedup
  WHERE rn_dedup = 1
)
SELECT
  CAST(transaction_id AS STRING) AS order_id,
  CAST(
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY product_id, transaction_time, store_id
    ) AS INT
  ) AS order_item_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(
    CASE
      WHEN COALESCE(quantity, 1) < 0 THEN 0
      ELSE COALESCE(quantity, 1)
    END AS INT
  ) AS quantity,
  CAST(
    CASE
      WHEN COALESCE(quantity, 0) > 0 THEN sale_amount / quantity
      ELSE NULL
    END AS DECIMAL(38, 10)
  ) AS unit_price_amount,
  CAST(sale_amount AS DECIMAL(38, 10)) AS line_total_amount
FROM kept
"""

order_items_silver_df = spark.sql(order_items_sql)

# WRITE: single CSV directly under TARGET_PATH (no subfolders)
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: silver.etl_run_log
# - Standardize:
#   status normalized to {SUCCESS, FAILED, RUNNING}
# - Dedup rule: keep latest entry per (etl_run_id) by max(end_ts, start_ts)
# -----------------------------------------------------------------------------------
etl_run_log_sql = """
WITH base AS (
  SELECT
    CAST(erl.etl_run_id AS STRING)            AS etl_run_id,
    CAST(erl.pipeline_name AS STRING)         AS pipeline_name,
    CAST(erl.run_date AS DATE)                AS run_date,
    CAST(erl.start_ts AS TIMESTAMP)           AS start_ts,
    CAST(erl.end_ts AS TIMESTAMP)             AS end_ts,
    CAST(erl.status AS STRING)                AS status_raw,
    CAST(erl.records_received AS BIGINT)      AS records_received,
    CAST(erl.records_valid AS BIGINT)         AS records_valid,
    CAST(erl.records_rejected AS BIGINT)      AS records_rejected,
    CAST(erl.error_message AS STRING)         AS error_message,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(erl.etl_run_id AS STRING)
      ORDER BY COALESCE(CAST(erl.end_ts AS TIMESTAMP), CAST(erl.start_ts AS TIMESTAMP)) DESC
    ) AS rn
  FROM etl_run_log erl
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
)
SELECT
  etl_run_id,
  pipeline_name,
  run_date,
  start_ts,
  end_ts,
  CASE
    WHEN UPPER(TRIM(status_raw)) IN ('SUCCESS', 'SUCCEEDED', 'OK', 'COMPLETED', 'COMPLETE') THEN 'SUCCESS'
    WHEN UPPER(TRIM(status_raw)) IN ('FAILED', 'FAIL', 'ERROR', 'ERRORS') THEN 'FAILED'
    WHEN UPPER(TRIM(status_raw)) IN ('RUNNING', 'IN_PROGRESS', 'INPROGRESS', 'STARTED') THEN 'RUNNING'
    ELSE UPPER(TRIM(status_raw))
  END AS status,
  records_received,
  records_valid,
  records_rejected,
  error_message
FROM dedup
"""

etl_run_log_silver_df = spark.sql(etl_run_log_sql)

# WRITE: single CSV directly under TARGET_PATH (no subfolders)
(
    etl_run_log_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_run_log.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: silver.data_quality_results
# - Standardize:
#   severity normalized to {LOW, MEDIUM, HIGH, CRITICAL}
#   failed_record_count cast to integer, non-negative
# - Dedup rule:
#   If timestamp exists keep latest per (etl_run_id, rule_name); otherwise aggregate sums.
#   (Implemented as: compute both paths; if timestamp is null, rank will not win and aggregation will apply per key.)
# -----------------------------------------------------------------------------------
data_quality_results_sql = """
WITH typed AS (
  SELECT
    CAST(dqr.etl_run_id AS STRING)            AS etl_run_id,
    CAST(dqr.rule_name AS STRING)             AS rule_name,
    CAST(dqr.rule_description AS STRING)      AS rule_description,
    CAST(dqr.severity AS STRING)              AS severity_raw,
    CAST(dqr.failed_record_count AS INT)      AS failed_record_count_raw,
    CAST(dqr.result_ts AS TIMESTAMP)          AS result_ts
  FROM data_quality_results dqr
),
normalized AS (
  SELECT
    etl_run_id,
    rule_name,
    rule_description,
    CASE
      WHEN UPPER(TRIM(severity_raw)) IN ('LOW', 'L') THEN 'LOW'
      WHEN UPPER(TRIM(severity_raw)) IN ('MEDIUM', 'MED', 'M') THEN 'MEDIUM'
      WHEN UPPER(TRIM(severity_raw)) IN ('HIGH', 'H') THEN 'HIGH'
      WHEN UPPER(TRIM(severity_raw)) IN ('CRITICAL', 'CRIT', 'C') THEN 'CRITICAL'
      ELSE UPPER(TRIM(severity_raw))
    END AS severity,
    CAST(
      CASE
        WHEN COALESCE(failed_record_count_raw, 0) < 0 THEN 0
        ELSE COALESCE(failed_record_count_raw, 0)
      END AS INT
    ) AS failed_record_count,
    result_ts
  FROM typed
),
latest_if_ts AS (
  SELECT
    n.*,
    ROW_NUMBER() OVER (
      PARTITION BY n.etl_run_id, n.rule_name
      ORDER BY n.result_ts DESC
    ) AS rn
  FROM normalized n
  WHERE n.result_ts IS NOT NULL
),
picked_latest AS (
  SELECT
    etl_run_id,
    rule_name,
    rule_description,
    severity,
    failed_record_count
  FROM latest_if_ts
  WHERE rn = 1
),
summed_no_ts AS (
  SELECT
    etl_run_id,
    rule_name,
    MAX(rule_description) AS rule_description,
    MAX(severity)         AS severity,
    CAST(SUM(failed_record_count) AS INT) AS failed_record_count
  FROM normalized
  WHERE result_ts IS NULL
  GROUP BY etl_run_id, rule_name
),
final AS (
  SELECT * FROM picked_latest
  UNION ALL
  SELECT * FROM summed_no_ts
)
SELECT
  etl_run_id,
  rule_name,
  rule_description,
  severity,
  failed_record_count
FROM final
"""

data_quality_results_silver_df = spark.sql(data_quality_results_sql)

# WRITE: single CSV directly under TARGET_PATH (no subfolders)
(
    data_quality_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_results.csv")
)

job.commit()
```