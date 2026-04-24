```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Job params (optional)
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ----------------------------
# Glue / Spark setup
# ----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Constants
# ----------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =====================================================================================
# SOURCE: silver.ingestion_file_audit_silver  (alias: ifas)
# TARGET: gold.gold_ingestion_file_audit
# =====================================================================================
df_ifas = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_file_audit_silver.{FILE_FORMAT}/")
)
df_ifas.createOrReplaceTempView("ingestion_file_audit_silver")

sql_gold_ingestion_file_audit = """
WITH base AS (
  SELECT
    CAST(ifas.audit_id AS BIGINT)              AS audit_id,
    CAST(ifas.source_system AS STRING)         AS source_system,
    CAST(ifas.s3_bucket AS STRING)             AS s3_bucket,
    CAST(ifas.s3_key AS STRING)                AS s3_key,
    CAST(ifas.file_name AS STRING)             AS file_name,
    CAST(ifas.file_format AS STRING)           AS file_format,
    CAST(ifas.file_size_bytes AS BIGINT)       AS file_size_bytes,
    CAST(ifas.file_row_count AS BIGINT)        AS file_row_count,
    CAST(ifas.file_checksum AS STRING)         AS file_checksum,
    CAST(ifas.ingestion_status AS STRING)      AS ingestion_status,
    CAST(ifas.ingestion_started_at AS TIMESTAMP)   AS ingestion_started_at,
    CAST(ifas.ingestion_completed_at AS TIMESTAMP) AS ingestion_completed_at,
    CAST(ifas.records_loaded AS BIGINT)        AS records_loaded,
    CAST(ifas.records_rejected AS BIGINT)      AS records_rejected,
    CAST(ifas.error_count AS BIGINT)           AS error_count,
    CAST(ifas.error_message AS STRING)         AS error_message,
    CAST(ifas.redshift_schema AS STRING)       AS redshift_schema,
    CAST(ifas.redshift_table AS STRING)        AS redshift_table,
    CAST(ifas.batch_id AS STRING)              AS batch_id,
    CAST(ifas.created_at AS TIMESTAMP)         AS created_at
  FROM ingestion_file_audit_silver ifas
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.s3_bucket, b.s3_key, b.file_checksum, b.batch_id
      ORDER BY b.created_at DESC
    ) AS rn
  FROM base b
)
SELECT
  audit_id,
  source_system,
  s3_bucket,
  s3_key,
  file_name,
  file_format,
  file_size_bytes,
  file_row_count,
  file_checksum,
  ingestion_status,
  ingestion_started_at,
  ingestion_completed_at,
  records_loaded,
  records_rejected,
  error_count,
  error_message,
  redshift_schema,
  redshift_table,
  batch_id,
  created_at
FROM dedup
WHERE rn = 1
"""
df_gold_ingestion_file_audit = spark.sql(sql_gold_ingestion_file_audit)

(
    df_gold_ingestion_file_audit.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_ingestion_file_audit.csv")
)

# =====================================================================================
# SOURCE: silver.ingestion_batch_summary_silver  (alias: ibss)
# TARGET: gold.gold_ingestion_batch_summary
# =====================================================================================
df_ibss = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_batch_summary_silver.{FILE_FORMAT}/")
)
df_ibss.createOrReplaceTempView("ingestion_batch_summary_silver")

sql_gold_ingestion_batch_summary = """
WITH base AS (
  SELECT
    CAST(ibss.batch_id AS STRING)              AS batch_id,
    CAST(ibss.source_system AS STRING)         AS source_system,
    CAST(ibss.batch_started_at AS TIMESTAMP)   AS batch_started_at,
    CAST(ibss.batch_completed_at AS TIMESTAMP) AS batch_completed_at,
    CAST(ibss.batch_status AS STRING)          AS batch_status,
    CAST(ibss.files_processed AS BIGINT)       AS files_processed,
    CAST(ibss.files_succeeded AS BIGINT)       AS files_succeeded,
    CAST(ibss.files_failed AS BIGINT)          AS files_failed,
    CAST(ibss.total_records_loaded AS BIGINT)  AS total_records_loaded,
    CAST(ibss.total_records_rejected AS BIGINT) AS total_records_rejected,
    CAST(ibss.total_error_count AS BIGINT)     AS total_error_count,
    CAST(ibss.sla_target_pct AS DECIMAL(5,2))  AS sla_target_pct,
    CAST(ibss.sla_achieved_flag AS BOOLEAN)    AS sla_achieved_flag,
    CAST(ibss.created_at AS TIMESTAMP)         AS created_at
  FROM ingestion_batch_summary_silver ibss
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.batch_id
      ORDER BY b.created_at DESC
    ) AS rn
  FROM base b
)
SELECT
  batch_id,
  source_system,
  batch_started_at,
  batch_completed_at,
  batch_status,
  files_processed,
  files_succeeded,
  files_failed,
  total_records_loaded,
  total_records_rejected,
  total_error_count,
  sla_target_pct,
  sla_achieved_flag,
  created_at
FROM dedup
WHERE rn = 1
"""
df_gold_ingestion_batch_summary = spark.sql(sql_gold_ingestion_batch_summary)

(
    df_gold_ingestion_batch_summary.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_ingestion_batch_summary.csv")
)

# =====================================================================================
# SOURCE: silver.data_quality_results_silver (alias: dqrs)
#         JOIN silver.ingestion_batch_summary_silver (alias: ibss)
# TARGET: gold.gold_data_quality_results
# =====================================================================================
df_dqrs = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_results_silver.{FILE_FORMAT}/")
)
df_dqrs.createOrReplaceTempView("data_quality_results_silver")

# (re)create view for join safety (already created above, but kept explicit per-table)
df_ibss.createOrReplaceTempView("ingestion_batch_summary_silver")

sql_gold_data_quality_results = """
WITH base AS (
  SELECT
    CAST(dqrs.dq_result_id AS BIGINT)          AS dq_result_id,
    CAST(dqrs.batch_id AS STRING)             AS batch_id,
    CAST(dqrs.source_system AS STRING)        AS source_system,
    CAST(dqrs.redshift_schema AS STRING)      AS redshift_schema,
    CAST(dqrs.redshift_table AS STRING)       AS redshift_table,
    CAST(dqrs.check_name AS STRING)           AS check_name,
    CAST(dqrs.check_type AS STRING)           AS check_type,
    CAST(dqrs.check_status AS STRING)         AS check_status,
    CAST(dqrs.failed_record_count AS BIGINT)  AS failed_record_count,
    CAST(dqrs.failed_pct AS DECIMAL(9,4))     AS failed_pct,
    CAST(dqrs.severity AS STRING)             AS severity,
    CAST(dqrs.executed_at AS TIMESTAMP)       AS executed_at,
    CAST(dqrs.details AS STRING)              AS details
  FROM data_quality_results_silver dqrs
  INNER JOIN ingestion_batch_summary_silver ibss
    ON dqrs.batch_id = ibss.batch_id
   AND dqrs.source_system = ibss.source_system
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.dq_result_id
      ORDER BY b.executed_at DESC
    ) AS rn
  FROM base b
)
SELECT
  dq_result_id,
  batch_id,
  source_system,
  redshift_schema,
  redshift_table,
  check_name,
  check_type,
  check_status,
  failed_record_count,
  failed_pct,
  severity,
  executed_at,
  details
FROM dedup
WHERE rn = 1
"""
df_gold_data_quality_results = spark.sql(sql_gold_data_quality_results)

(
    df_gold_data_quality_results.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_results.csv")
)

# =====================================================================================
# SOURCE: silver.sensitive_data_access_audit_silver (alias: sdaas)
# TARGET: gold.gold_sensitive_data_access_audit
# =====================================================================================
df_sdaas = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sensitive_data_access_audit_silver.{FILE_FORMAT}/")
)
df_sdaas.createOrReplaceTempView("sensitive_data_access_audit_silver")

sql_gold_sensitive_data_access_audit = """
WITH base AS (
  SELECT
    CAST(sdaas.access_audit_id AS BIGINT)         AS access_audit_id,
    CAST(sdaas.event_time AS TIMESTAMP)           AS event_time,
    CAST(sdaas.actor AS STRING)                   AS actor,
    CAST(sdaas.actor_role AS STRING)              AS actor_role,
    CAST(sdaas.action AS STRING)                  AS action,
    CAST(sdaas.object_type AS STRING)             AS object_type,
    CAST(sdaas.redshift_schema AS STRING)         AS redshift_schema,
    CAST(sdaas.redshift_table AS STRING)          AS redshift_table,
    CAST(sdaas.column_name AS STRING)             AS column_name,
    CAST(sdaas.sensitivity_classification AS STRING) AS sensitivity_classification,
    CAST(sdaas.decision AS STRING)                AS decision,
    CAST(sdaas.reason AS STRING)                  AS reason,
    CAST(sdaas.request_id AS STRING)              AS request_id
  FROM sensitive_data_access_audit_silver sdaas
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.request_id, b.actor, b.action, b.object_type, b.redshift_schema, b.redshift_table, b.column_name, b.event_time
      ORDER BY b.event_time DESC, b.access_audit_id DESC
    ) AS rn
  FROM base b
)
SELECT
  access_audit_id,
  event_time,
  actor,
  actor_role,
  action,
  object_type,
  redshift_schema,
  redshift_table,
  column_name,
  sensitivity_classification,
  decision,
  reason,
  request_id
FROM dedup
WHERE rn = 1
"""
df_gold_sensitive_data_access_audit = spark.sql(sql_gold_sensitive_data_access_audit)

(
    df_gold_sensitive_data_access_audit.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sensitive_data_access_audit.csv")
)

job.commit()
```