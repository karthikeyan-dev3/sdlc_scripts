```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue bootstrap
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

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Common CSV read options (bronze)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\"",
    "mode": "PERMISSIVE",
}

# ===================================================================================
# SOURCE READS + TEMP VIEWS
# ===================================================================================

# bronze.customer_orders
df_bronze_customer_orders = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_bronze_customer_orders.createOrReplaceTempView("bronze_customer_orders")

# bronze.customer_order_line_items
df_bronze_customer_order_line_items = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_line_items.{FILE_FORMAT}/")
)
df_bronze_customer_order_line_items.createOrReplaceTempView("bronze_customer_order_line_items")

# bronze.etl_batch_run_metadata
df_bronze_etl_batch_run_metadata = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/etl_batch_run_metadata.{FILE_FORMAT}/")
)
df_bronze_etl_batch_run_metadata.createOrReplaceTempView("bronze_etl_batch_run_metadata")

# bronze.data_quality_results
df_bronze_data_quality_results = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/data_quality_results.{FILE_FORMAT}/")
)
df_bronze_data_quality_results.createOrReplaceTempView("bronze_data_quality_results")

# bronze.customer_sensitive_access_map
df_bronze_customer_sensitive_access_map = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_sensitive_access_map.{FILE_FORMAT}/")
)
df_bronze_customer_sensitive_access_map.createOrReplaceTempView("bronze_customer_sensitive_access_map")

# ===================================================================================
# TARGET: silver.customer_orders
#   - deduplicate by transaction_id
#   - order_date = DATE(transaction_time)
#   - currency_code default 'USD'
#   - order_status = COMPLETED when sale_amount not null else PENDING
#   - source_system = 'sales_transactions_raw'
#   - ingestion_date = CAST(transaction_time AS DATE)
# ===================================================================================
df_customer_orders = spark.sql("""
WITH base AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.transaction_id
      ORDER BY co.transaction_time DESC
    ) AS rn
  FROM bronze_customer_orders co
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
)
SELECT
  CAST(co.transaction_id AS STRING)                                         AS order_id,
  DATE(co.transaction_time)                                                 AS order_date,
  CAST(co.store_id AS STRING)                                               AS customer_id,
  CASE WHEN co.sale_amount IS NOT NULL THEN 'COMPLETED' ELSE 'PENDING' END  AS order_status,
  'USD'                                                                     AS currency_code,
  CAST(co.sale_amount AS DECIMAL(38, 10))                                   AS order_total_amount,
  'sales_transactions_raw'                                                  AS source_system,
  CAST(co.transaction_time AS DATE)                                         AS ingestion_date
FROM dedup co
""")

(
    df_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TARGET: silver.customer_order_line_items
#   - deduplicate by transaction_id, product_id
#   - order_line_id = MD5(transaction_id||'|'||COALESCE(product_id,'UNKNOWN')||'|'||COALESCE(CAST(quantity AS VARCHAR),''))
#   - unit_price_amount = sale_amount/quantity when quantity>0
#   - line_total_amount = COALESCE(sale_amount,0)
#   - ingestion_date = CURRENT_DATE
# ===================================================================================
df_customer_order_line_items = spark.sql("""
WITH base AS (
  SELECT
    coli.*,
    ROW_NUMBER() OVER (
      PARTITION BY coli.transaction_id, coli.product_id
      ORDER BY coli.transaction_id
    ) AS rn
  FROM bronze_customer_order_line_items coli
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
)
SELECT
  CAST(coli.transaction_id AS STRING) AS order_id,
  md5(
    concat(
      CAST(coli.transaction_id AS STRING),
      '|',
      COALESCE(CAST(coli.product_id AS STRING), 'UNKNOWN'),
      '|',
      COALESCE(CAST(coli.quantity AS STRING), '')
    )
  ) AS order_line_id,
  COALESCE(CAST(coli.product_id AS STRING), 'UNKNOWN') AS product_id,
  CAST(COALESCE(coli.quantity, 0) AS INT) AS quantity,
  CASE
    WHEN coli.quantity > 0 THEN CAST(coli.sale_amount AS DECIMAL(38, 10)) / CAST(coli.quantity AS DECIMAL(38, 10))
    ELSE NULL
  END AS unit_price_amount,
  CAST(COALESCE(coli.sale_amount, 0) AS DECIMAL(38, 10)) AS line_total_amount,
  CURRENT_DATE AS ingestion_date
FROM dedup coli
""")

(
    df_customer_order_line_items.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_line_items.csv")
)

# ===================================================================================
# TARGET: silver.etl_batch_run_metadata
#   - group by batch_id, pipeline_run_id
#   - pipeline_name = COALESCE(pipeline_run_id,'sales_transactions_daily_load')
#   - run_date = CAST(ingest_start_ts AS DATE)
#   - source_s3_path = source_name (per columns UDT)
#   - start_timestamp = MIN(ingest_start_ts)
#   - end_timestamp = MAX(ingest_end_ts)
#   - records_read = 0, records_loaded = 0
#   - run_status = COMPLETED when end_timestamp not null else RUNNING
# ===================================================================================
df_etl_batch_run_metadata = spark.sql("""
WITH agg AS (
  SELECT
    CAST(ebrm.batch_id AS STRING) AS batch_id,
    CAST(ebrm.pipeline_run_id AS STRING) AS pipeline_run_id,
    MIN(ebrm.ingest_start_ts) AS start_timestamp,
    MAX(ebrm.ingest_end_ts)   AS end_timestamp,
    MIN(ebrm.source_name)     AS source_s3_path
  FROM bronze_etl_batch_run_metadata ebrm
  GROUP BY
    CAST(ebrm.batch_id AS STRING),
    CAST(ebrm.pipeline_run_id AS STRING)
)
SELECT
  batch_id AS batch_id,
  COALESCE(pipeline_run_id, 'sales_transactions_daily_load') AS pipeline_name,
  CAST(start_timestamp AS DATE) AS run_date,
  CAST(source_s3_path AS STRING) AS source_s3_path,
  CAST(start_timestamp AS TIMESTAMP) AS start_timestamp,
  CAST(end_timestamp AS TIMESTAMP) AS end_timestamp,
  CAST(0 AS BIGINT) AS records_read,
  CAST(0 AS BIGINT) AS records_loaded,
  CASE WHEN end_timestamp IS NOT NULL THEN 'COMPLETED' ELSE 'RUNNING' END AS run_status
FROM agg
""")

(
    df_etl_batch_run_metadata.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_batch_run_metadata.csv")
)

# ===================================================================================
# TARGET: silver.data_quality_results
#   - LEFT JOIN to batch metadata on batch_id
#   - run_date = CAST(ebrm.ingest_start_ts AS DATE)
#   - check_type = 'INGESTION'
#   - target_table = dqr.source_name
#   - records_evaluated = 0
#   - records_failed = COUNT(failed_record_id) OVER (PARTITION BY batch_id, check_name)
#   - quality_score_pct = 0
#   - result_status = PASS when check_status in ('PASS','PASSED') else FAIL
# ===================================================================================
df_data_quality_results = spark.sql("""
SELECT
  CAST(dqr.batch_id AS STRING) AS batch_id,
  CAST(ebrm.ingest_start_ts AS DATE) AS run_date,
  CAST(dqr.check_name AS STRING) AS check_name,
  'INGESTION' AS check_type,
  CAST(dqr.source_name AS STRING) AS target_table,
  CAST(0 AS BIGINT) AS records_evaluated,
  CAST(
    COUNT(dqr.failed_record_id) OVER (PARTITION BY dqr.batch_id, dqr.check_name)
    AS BIGINT
  ) AS records_failed,
  CAST(0 AS DECIMAL(38, 10)) AS quality_score_pct,
  CASE WHEN dqr.check_status IN ('PASS', 'PASSED') THEN 'PASS' ELSE 'FAIL' END AS result_status
FROM bronze_data_quality_results dqr
LEFT JOIN bronze_etl_batch_run_metadata ebrm
  ON dqr.batch_id = ebrm.batch_id
""")

(
    df_data_quality_results.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_results.csv")
)

# ===================================================================================
# TARGET: silver.customer_sensitive_access_map
#   - filter subject_type='CUSTOMER'
#   - customer_id = subject_id
#   - is_sensitive = access_level in ('MASK','SENSITIVE','RESTRICTED')
#   - masking_policy_name = 'default_customer_mask' when sensitive else NULL
#   - effective_start_date = effective_start_dt
#   - effective_end_date = effective_end_dt
#   - deduplicate by customer_id,effective_start_date keeping latest granted_ts
# ===================================================================================
df_customer_sensitive_access_map = spark.sql("""
WITH filtered AS (
  SELECT
    csam.*,
    ROW_NUMBER() OVER (
      PARTITION BY csam.subject_id, csam.effective_start_dt
      ORDER BY csam.granted_ts DESC
    ) AS rn
  FROM bronze_customer_sensitive_access_map csam
  WHERE csam.subject_type = 'CUSTOMER'
),
dedup AS (
  SELECT *
  FROM filtered
  WHERE rn = 1
)
SELECT
  CAST(csam.subject_id AS STRING) AS customer_id,
  CASE
    WHEN csam.access_level IN ('MASK','SENSITIVE','RESTRICTED') THEN TRUE
    ELSE FALSE
  END AS is_sensitive,
  CASE
    WHEN csam.access_level IN ('MASK','SENSITIVE','RESTRICTED') THEN 'default_customer_mask'
    ELSE NULL
  END AS masking_policy_name,
  CAST(csam.effective_start_dt AS DATE) AS effective_start_date,
  CAST(csam.effective_end_dt AS DATE) AS effective_end_date
FROM dedup csam
""")

(
    df_customer_sensitive_access_map.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_sensitive_access_map.csv")
)

job.commit()
```