```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV read options (kept explicit and consistent)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "true",
    "escape": '"'
}

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze) + create temp views
#    IMPORTANT: source reading path format must be: .load(f"{SOURCE_PATH}/table.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
df_bronze_customer_orders = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_bronze_customer_orders.createOrReplaceTempView("customer_orders")

df_bronze_customer_order_items = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_bronze_customer_order_items.createOrReplaceTempView("customer_order_items")

df_bronze_etl_run_audit = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/etl_run_audit.{FILE_FORMAT}/")
)
df_bronze_etl_run_audit.createOrReplaceTempView("etl_run_audit")

df_bronze_data_quality_results = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/data_quality_results.{FILE_FORMAT}/")
)
df_bronze_data_quality_results.createOrReplaceTempView("data_quality_results")

# ====================================================================================
# TARGET TABLE: silver.customer_orders
# ====================================================================================
customer_orders_sql = """
WITH base AS (
    SELECT
        -- Transformations (EXACT from UDT)
        co.transaction_id                                                AS order_id,
        CAST(co.transaction_time AS DATE)                                AS order_date,
        co.store_id                                                      AS customer_id,
        'COMPLETED'                                                      AS order_status,
        COALESCE(co.sale_amount, 0)                                      AS order_total_amount,
        'USD'                                                            AS currency_code,
        CAST(COALESCE(era.run_start_ts, co.transaction_time, CURRENT_DATE) AS DATE) AS ingestion_date,

        -- De-duplication: keep latest record per transaction_id ordered by transaction_time desc
        ROW_NUMBER() OVER (
            PARTITION BY co.transaction_id
            ORDER BY co.transaction_time DESC
        ) AS rn
    FROM customer_orders co
    LEFT JOIN etl_run_audit era
        ON 1 = 1
)
SELECT
    order_id,
    order_date,
    customer_id,
    order_status,
    order_total_amount,
    currency_code,
    ingestion_date
FROM base
WHERE rn = 1
  AND order_id IS NOT NULL
"""

df_customer_orders = spark.sql(customer_orders_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
df_customer_orders.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_orders.csv"
)

# ====================================================================================
# TARGET TABLE: silver.customer_order_items
# ====================================================================================
customer_order_items_sql = """
WITH base AS (
    SELECT
        -- Transformations (EXACT from UDT)
        coi.transaction_id AS order_id,
        MD5(
            CONCAT(
                coi.transaction_id, '|',
                COALESCE(coi.product_id, ''), '|',
                COALESCE(CAST(coi.quantity AS STRING), '')
            )
        ) AS order_item_id,
        coi.product_id AS product_id,
        COALESCE(coi.quantity, 0) AS quantity,

        CASE
            WHEN COALESCE(coi.quantity, 0) > 0 THEN
                (
                    COALESCE(co.sale_amount, 0)
                    / NULLIF(
                        SUM(COALESCE(coi.quantity, 0)) OVER (PARTITION BY coi.transaction_id),
                        0
                    )
                )
            ELSE 0
        END AS unit_price,

        (
            CASE
                WHEN COALESCE(coi.quantity, 0) > 0 THEN
                    (
                        COALESCE(co.sale_amount, 0)
                        / NULLIF(
                            SUM(COALESCE(coi.quantity, 0)) OVER (PARTITION BY coi.transaction_id),
                            0
                        )
                    )
                ELSE 0
            END
        ) * COALESCE(coi.quantity, 0) AS line_total_amount,

        -- De-duplication: keep one record per (transaction_id, product_id, quantity)
        ROW_NUMBER() OVER (
            PARTITION BY coi.transaction_id, coi.product_id, COALESCE(coi.quantity, 0)
            ORDER BY co.transaction_time DESC
        ) AS rn
    FROM customer_order_items coi
    INNER JOIN customer_orders co
        ON coi.transaction_id = co.transaction_id
)
SELECT
    order_id,
    order_item_id,
    product_id,
    quantity,
    unit_price,
    line_total_amount
FROM base
WHERE rn = 1
  AND order_id IS NOT NULL
"""

df_customer_order_items = spark.sql(customer_order_items_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
df_customer_order_items.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_order_items.csv"
)

# ====================================================================================
# TARGET TABLE: silver.etl_run_audit
# ====================================================================================
etl_run_audit_sql = """
WITH base AS (
    SELECT
        -- Transformations (EXACT from UDT)
        era.pipeline_name                         AS pipeline_name,
        era.etl_run_id                            AS run_id,
        CAST(era.run_start_ts AS DATE)            AS run_date,
        UPPER(era.status)                         AS status,
        COALESCE(era.rows_read, 0)                AS records_read,
        COALESCE(era.rows_written, 0)             AS records_loaded,
        CASE
            WHEN era.status IN ('FAILED', 'ERROR') OR era.error_message IS NOT NULL THEN 1
            ELSE 0
        END                                       AS error_count,
        era.run_start_ts                          AS started_at,
        era.run_end_ts                            AS ended_at,

        -- De-duplication: keep latest record per etl_run_id ordered by run_start_ts desc
        ROW_NUMBER() OVER (
            PARTITION BY era.etl_run_id
            ORDER BY era.run_start_ts DESC
        ) AS rn
    FROM etl_run_audit era
)
SELECT
    pipeline_name,
    run_id,
    run_date,
    status,
    records_read,
    records_loaded,
    error_count,
    started_at,
    ended_at
FROM base
WHERE rn = 1
"""

df_etl_run_audit = spark.sql(etl_run_audit_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
df_etl_run_audit.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/etl_run_audit.csv"
)

# ====================================================================================
# TARGET TABLE: silver.data_quality_results
# ====================================================================================
data_quality_results_sql = """
WITH base AS (
    SELECT
        -- Transformations (EXACT from UDT)
        dqr.etl_run_id                 AS run_id,
        dqr.rule_name                  AS check_name,
        UPPER(dqr.status)              AS check_status,
        COALESCE(dqr.records_failed, 0) AS failed_record_count,
        dqr.created_ts                 AS check_timestamp,

        -- De-duplication: keep latest record per (etl_run_id, rule_name, target_table) ordered by created_ts desc.
        -- Note: if target_table is not present in the bronze dataset, Spark will fail to resolve it.
        ROW_NUMBER() OVER (
            PARTITION BY dqr.etl_run_id, dqr.rule_name, dqr.target_table
            ORDER BY dqr.created_ts DESC
        ) AS rn
    FROM data_quality_results dqr
)
SELECT
    run_id,
    check_name,
    check_status,
    failed_record_count,
    check_timestamp
FROM base
WHERE rn = 1
"""

df_data_quality_results = spark.sql(data_quality_results_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
df_data_quality_results.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/data_quality_results.csv"
)

job.commit()
```