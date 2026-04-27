import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Read Source Tables
# ------------------------------------------------------------------------------------
silver_ingestion_kpi_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_ingestion_kpi.{FILE_FORMAT}/")
)
silver_ingestion_kpi_df.createOrReplaceTempView("silver_ingestion_kpi")

silver_pipeline_run_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_pipeline_run.{FILE_FORMAT}/")
)
silver_pipeline_run_df.createOrReplaceTempView("silver_pipeline_run")

silver_data_quality_score_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_data_quality_score.{FILE_FORMAT}/")
)
silver_data_quality_score_df.createOrReplaceTempView("silver_data_quality_score")

silver_dataset_catalog_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_dataset_catalog.{FILE_FORMAT}/")
)
silver_dataset_catalog_df.createOrReplaceTempView("silver_dataset_catalog")

silver_data_access_audit_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_data_access_audit.{FILE_FORMAT}/")
)
silver_data_access_audit_df.createOrReplaceTempView("silver_data_access_audit")

# ------------------------------------------------------------------------------------
# Target: gold_ingestion_kpi
# Mapping: silver.silver_ingestion_kpi sik
# ------------------------------------------------------------------------------------
gold_ingestion_kpi_sql = """
SELECT
  CAST(sik.dataset_id AS STRING) AS dataset_id,
  CAST(sik.source_system AS STRING) AS source_system,
  CAST(sik.ingestion_method AS STRING) AS ingestion_method,
  CAST(sik.load_type AS STRING) AS load_type,
  CAST(sik.event_date AS DATE) AS event_date,
  CAST(sik.batch_id AS STRING) AS batch_id,
  CAST(sik.ingest_start_ts AS TIMESTAMP) AS ingest_start_ts,
  CAST(sik.ingest_end_ts AS TIMESTAMP) AS ingest_end_ts,
  CAST(sik.ingestion_latency_minutes AS DECIMAL(38,18)) AS ingestion_latency_minutes,
  CAST(sik.records_ingested AS INT) AS records_ingested,
  CAST(sik.bytes_ingested AS DECIMAL(38,18)) AS bytes_ingested,
  CAST(sik.ingestion_status AS STRING) AS ingestion_status
FROM silver_ingestion_kpi sik
"""
gold_ingestion_kpi_df = spark.sql(gold_ingestion_kpi_sql)

(
    gold_ingestion_kpi_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_ingestion_kpi.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_pipeline_run
# Mapping: silver.silver_pipeline_run spr
# ------------------------------------------------------------------------------------
gold_pipeline_run_sql = """
SELECT
  CAST(spr.pipeline_id AS STRING) AS pipeline_id,
  CAST(spr.pipeline_name AS STRING) AS pipeline_name,
  CAST(spr.orchestrator_tool AS STRING) AS orchestrator_tool,
  CAST(spr.run_id AS STRING) AS run_id,
  CAST(spr.trigger_type AS STRING) AS trigger_type,
  CAST(spr.run_start_ts AS TIMESTAMP) AS run_start_ts,
  CAST(spr.run_end_ts AS TIMESTAMP) AS run_end_ts,
  CAST(spr.run_status AS STRING) AS run_status,
  CAST(spr.error_code AS STRING) AS error_code,
  CAST(spr.error_message AS STRING) AS error_message,
  CAST(spr.records_processed AS INT) AS records_processed,
  CAST(spr.sla_minutes AS INT) AS sla_minutes,
  CAST(spr.sla_met_flag AS BOOLEAN) AS sla_met_flag
FROM silver_pipeline_run spr
"""
gold_pipeline_run_df = spark.sql(gold_pipeline_run_sql)

(
    gold_pipeline_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_pipeline_run.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_data_quality_score
# Mapping: silver.silver_data_quality_score sdqs
# ------------------------------------------------------------------------------------
gold_data_quality_score_sql = """
SELECT
  CAST(sdqs.dataset_id AS STRING) AS dataset_id,
  CAST(sdqs.check_suite_id AS STRING) AS check_suite_id,
  CAST(sdqs.check_run_id AS STRING) AS check_run_id,
  CAST(sdqs.check_date AS DATE) AS check_date,
  CAST(sdqs.check_start_ts AS TIMESTAMP) AS check_start_ts,
  CAST(sdqs.check_end_ts AS TIMESTAMP) AS check_end_ts,
  CAST(sdqs.total_checks AS INT) AS total_checks,
  CAST(sdqs.passed_checks AS INT) AS passed_checks,
  CAST(sdqs.failed_checks AS INT) AS failed_checks,
  CAST(sdqs.quality_score_pct AS DECIMAL(38,18)) AS quality_score_pct,
  CAST(sdqs.severity_high_fail_count AS INT) AS severity_high_fail_count,
  CAST(sdqs.severity_medium_fail_count AS INT) AS severity_medium_fail_count,
  CAST(sdqs.severity_low_fail_count AS INT) AS severity_low_fail_count
FROM silver_data_quality_score sdqs
"""
gold_data_quality_score_df = spark.sql(gold_data_quality_score_sql)

(
    gold_data_quality_score_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_score.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_dataset_catalog
# Mapping: silver.silver_dataset_catalog sdc
# ------------------------------------------------------------------------------------
gold_dataset_catalog_sql = """
SELECT
  CAST(sdc.dataset_id AS STRING) AS dataset_id,
  CAST(sdc.dataset_name AS STRING) AS dataset_name,
  CAST(sdc.data_layer AS STRING) AS data_layer,
  CAST(sdc.domain AS STRING) AS domain,
  CAST(sdc.owner_team AS STRING) AS owner_team,
  CAST(sdc.description AS STRING) AS description,
  CAST(sdc.source_system AS STRING) AS source_system,
  CAST(sdc.ingestion_method AS STRING) AS ingestion_method,
  CAST(sdc.update_frequency AS STRING) AS update_frequency,
  CAST(sdc.pii_flag AS BOOLEAN) AS pii_flag,
  CAST(sdc.classification AS STRING) AS classification,
  CAST(sdc.retention_days AS INT) AS retention_days,
  CAST(sdc.schema_version AS STRING) AS schema_version,
  CAST(sdc.cataloged_ts AS TIMESTAMP) AS cataloged_ts,
  CAST(sdc.active_flag AS BOOLEAN) AS active_flag
FROM silver_dataset_catalog sdc
"""
gold_dataset_catalog_df = spark.sql(gold_dataset_catalog_sql)

(
    gold_dataset_catalog_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dataset_catalog.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_data_access_audit
# Mapping: silver.silver_data_access_audit sdaa
# ------------------------------------------------------------------------------------
gold_data_access_audit_sql = """
SELECT
  CAST(sdaa.request_id AS STRING) AS request_id,
  CAST(sdaa.request_ts AS TIMESTAMP) AS request_ts,
  CAST(sdaa.principal_id AS STRING) AS principal_id,
  CAST(sdaa.principal_type AS STRING) AS principal_type,
  CAST(sdaa.api_endpoint AS STRING) AS api_endpoint,
  CAST(sdaa.http_method AS STRING) AS http_method,
  CAST(sdaa.dataset_id AS STRING) AS dataset_id,
  CAST(sdaa.action AS STRING) AS action,
  CAST(sdaa.authz_decision AS STRING) AS authz_decision,
  CAST(sdaa.response_status_code AS INT) AS response_status_code,
  CAST(sdaa.response_time_ms AS INT) AS response_time_ms
FROM silver_data_access_audit sdaa
"""
gold_data_access_audit_df = spark.sql(gold_data_access_audit_sql)

(
    gold_data_access_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_access_audit.csv")
)

job.commit()
