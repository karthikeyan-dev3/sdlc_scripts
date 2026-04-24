```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended settings for CSV I/O
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ======================================================================================
# SOURCE: bronze.ingestion_file_audit_bronze  -> TARGET: ingestion_file_audit_silver
# ======================================================================================
ingestion_file_audit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .load(f"{SOURCE_PATH}/ingestion_file_audit_bronze.{FILE_FORMAT}/")
)
ingestion_file_audit_bronze_df.createOrReplaceTempView("ingestion_file_audit_bronze_df")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bronze__ingestion_file_audit_bronze AS
SELECT * FROM ingestion_file_audit_bronze_df
""")

ingestion_file_audit_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(audit_id),'') AS BIGINT) AS audit_id,
    NULLIF(TRIM(source_system),'') AS source_system,
    NULLIF(TRIM(s3_bucket),'') AS s3_bucket,
    NULLIF(TRIM(s3_key),'') AS s3_key,
    NULLIF(TRIM(file_name),'') AS file_name,
    LOWER(NULLIF(TRIM(file_format),'')) AS file_format,
    CAST(NULLIF(TRIM(file_size_bytes),'') AS BIGINT) AS file_size_bytes,
    CAST(NULLIF(TRIM(file_row_count),'') AS BIGINT) AS file_row_count,
    NULLIF(TRIM(file_checksum),'') AS file_checksum,
    UPPER(NULLIF(TRIM(ingestion_status),'')) AS ingestion_status,
    CAST(ingestion_started_at AS TIMESTAMP) AS ingestion_started_at,
    CAST(ingestion_completed_at AS TIMESTAMP) AS ingestion_completed_at,
    CAST(NULLIF(TRIM(records_loaded),'') AS BIGINT) AS records_loaded,
    CAST(NULLIF(TRIM(records_rejected),'') AS BIGINT) AS records_rejected,
    CAST(NULLIF(TRIM(error_count),'') AS BIGINT) AS error_count,
    NULLIF(TRIM(error_message),'') AS error_message,
    NULLIF(TRIM(redshift_schema),'') AS redshift_schema,
    NULLIF(TRIM(redshift_table),'') AS redshift_table,
    NULLIF(TRIM(batch_id),'') AS batch_id,
    CAST(created_at AS TIMESTAMP) AS created_at
  FROM bronze__ingestion_file_audit_bronze
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(s3_bucket,'~'),
        COALESCE(s3_key,'~'),
        COALESCE(file_checksum,'~'),
        COALESCE(batch_id,'~')
      ORDER BY created_at DESC, ingestion_started_at DESC
    ) AS rn
  FROM base
)
SELECT DISTINCT
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
""")

(
    ingestion_file_audit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestion_file_audit_silver.csv")
)

# ======================================================================================
# SOURCE: bronze.ingestion_batch_summary_bronze  -> TARGET: ingestion_batch_summary_silver
# ======================================================================================
ingestion_batch_summary_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .load(f"{SOURCE_PATH}/ingestion_batch_summary_bronze.{FILE_FORMAT}/")
)
ingestion_batch_summary_bronze_df.createOrReplaceTempView("ingestion_batch_summary_bronze_df")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bronze__ingestion_batch_summary_bronze AS
SELECT * FROM ingestion_batch_summary_bronze_df
""")

ingestion_batch_summary_silver_df = spark.sql("""
WITH base AS (
  SELECT
    NULLIF(TRIM(batch_id),'') AS batch_id,
    NULLIF(TRIM(source_system),'') AS source_system,
    CAST(batch_started_at AS TIMESTAMP) AS batch_started_at,
    CAST(batch_completed_at AS TIMESTAMP) AS batch_completed_at,
    UPPER(NULLIF(TRIM(batch_status),'')) AS batch_status,
    CAST(NULLIF(TRIM(files_processed),'') AS BIGINT) AS files_processed,
    CAST(NULLIF(TRIM(files_succeeded),'') AS BIGINT) AS files_succeeded,
    CAST(NULLIF(TRIM(files_failed),'') AS BIGINT) AS files_failed,
    CAST(NULLIF(TRIM(total_records_loaded),'') AS BIGINT) AS total_records_loaded,
    CAST(NULLIF(TRIM(total_records_rejected),'') AS BIGINT) AS total_records_rejected,
    CAST(NULLIF(TRIM(total_error_count),'') AS BIGINT) AS total_error_count,
    CAST(NULLIF(TRIM(sla_target_pct),'') AS DECIMAL(5,2)) AS sla_target_pct,
    CASE
      WHEN UPPER(NULLIF(TRIM(sla_achieved_flag),'')) IN ('Y','YES','TRUE','1') THEN TRUE
      WHEN UPPER(NULLIF(TRIM(sla_achieved_flag),'')) IN ('N','NO','FALSE','0') THEN FALSE
      ELSE NULL
    END AS sla_achieved_flag,
    CAST(created_at AS TIMESTAMP) AS created_at
  FROM bronze__ingestion_batch_summary_bronze
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(batch_id,'~')
      ORDER BY created_at DESC, batch_started_at DESC
    ) AS rn
  FROM base
)
SELECT DISTINCT
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
""")

(
    ingestion_batch_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestion_batch_summary_silver.csv")
)

# ======================================================================================
# SOURCE: bronze.data_quality_results_bronze  -> TARGET: data_quality_results_silver
# ======================================================================================
data_quality_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .load(f"{SOURCE_PATH}/data_quality_results_bronze.{FILE_FORMAT}/")
)
data_quality_results_bronze_df.createOrReplaceTempView("data_quality_results_bronze_df")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bronze__data_quality_results_bronze AS
SELECT * FROM data_quality_results_bronze_df
""")

data_quality_results_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(dq_result_id),'') AS BIGINT) AS dq_result_id,
    NULLIF(TRIM(batch_id),'') AS batch_id,
    NULLIF(TRIM(source_system),'') AS source_system,
    NULLIF(TRIM(redshift_schema),'') AS redshift_schema,
    NULLIF(TRIM(redshift_table),'') AS redshift_table,
    NULLIF(TRIM(check_name),'') AS check_name,
    UPPER(NULLIF(TRIM(check_type),'')) AS check_type,
    UPPER(NULLIF(TRIM(check_status),'')) AS check_status,
    CAST(NULLIF(TRIM(failed_record_count),'') AS BIGINT) AS failed_record_count,
    CAST(NULLIF(TRIM(failed_pct),'') AS DECIMAL(9,4)) AS failed_pct,
    UPPER(NULLIF(TRIM(severity),'')) AS severity,
    CAST(executed_at AS TIMESTAMP) AS executed_at,
    NULLIF(TRIM(details),'') AS details
  FROM bronze__data_quality_results_bronze
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(batch_id,'~'),
        COALESCE(source_system,'~'),
        COALESCE(redshift_schema,'~'),
        COALESCE(redshift_table,'~'),
        COALESCE(check_name,'~'),
        COALESCE(executed_at, TIMESTAMP '1900-01-01')
      ORDER BY executed_at DESC, dq_result_id DESC
    ) AS rn
  FROM base
)
SELECT DISTINCT
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
""")

(
    data_quality_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_results_silver.csv")
)

# ======================================================================================
# SOURCE: bronze.sensitive_data_access_audit_bronze  -> TARGET: sensitive_data_access_audit_silver
# ======================================================================================
sensitive_data_access_audit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .load(f"{SOURCE_PATH}/sensitive_data_access_audit_bronze.{FILE_FORMAT}/")
)
sensitive_data_access_audit_bronze_df.createOrReplaceTempView("sensitive_data_access_audit_bronze_df")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bronze__sensitive_data_access_audit_bronze AS
SELECT * FROM sensitive_data_access_audit_bronze_df
""")

sensitive_data_access_audit_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(NULLIF(TRIM(access_audit_id),'') AS BIGINT) AS access_audit_id,
    CAST(event_time AS TIMESTAMP) AS event_time,
    NULLIF(TRIM(actor),'') AS actor,
    NULLIF(TRIM(actor_role),'') AS actor_role,
    UPPER(NULLIF(TRIM(action),'')) AS action,
    UPPER(NULLIF(TRIM(object_type),'')) AS object_type,
    NULLIF(TRIM(redshift_schema),'') AS redshift_schema,
    NULLIF(TRIM(redshift_table),'') AS redshift_table,
    NULLIF(TRIM(column_name),'') AS column_name,
    UPPER(NULLIF(TRIM(sensitivity_classification),'')) AS sensitivity_classification,
    UPPER(NULLIF(TRIM(decision),'')) AS decision,
    NULLIF(TRIM(reason),'') AS reason,
    NULLIF(TRIM(request_id),'') AS request_id
  FROM bronze__sensitive_data_access_audit_bronze
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(request_id,'~'),
        COALESCE(actor,'~'),
        COALESCE(action,'~'),
        COALESCE(redshift_schema,'~'),
        COALESCE(redshift_table,'~'),
        COALESCE(column_name,'~'),
        COALESCE(event_time, TIMESTAMP '1900-01-01')
      ORDER BY event_time DESC, access_audit_id DESC
    ) AS rn
  FROM base
)
SELECT DISTINCT
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
""")

(
    sensitive_data_access_audit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sensitive_data_access_audit_silver.csv")
)

job.commit()
```