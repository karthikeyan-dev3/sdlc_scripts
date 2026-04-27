import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# Read source tables (Bronze)
# ----------------------------
pipeline_run_metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/pipeline_run_metrics_bronze.{FILE_FORMAT}/")
)
pipeline_run_metrics_bronze_df.createOrReplaceTempView("pipeline_run_metrics_bronze")

data_quality_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_results_bronze.{FILE_FORMAT}/")
)
data_quality_results_bronze_df.createOrReplaceTempView("data_quality_results_bronze")

data_catalog_assets_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_catalog_assets_bronze.{FILE_FORMAT}/")
)
data_catalog_assets_bronze_df.createOrReplaceTempView("data_catalog_assets_bronze")

data_access_audit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_access_audit_bronze.{FILE_FORMAT}/")
)
data_access_audit_bronze_df.createOrReplaceTempView("data_access_audit_bronze")

# ============================================================
# Target: silver.pipeline_run_metrics_silver
# ============================================================
pipeline_run_metrics_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            CAST(prm_b.pipeline_name AS STRING) AS pipeline_name,
            CAST(prm_b.run_id AS STRING) AS run_id,
            CAST(prm_b.run_start_ts AS TIMESTAMP) AS run_start_ts,
            CAST(prm_b.run_end_ts AS TIMESTAMP) AS run_end_ts,
            CAST(prm_b.run_status AS STRING) AS run_status,
            CAST(prm_b.records_read AS BIGINT) AS records_read,
            CAST(prm_b.records_written AS BIGINT) AS records_written,
            CAST((unix_timestamp(CAST(prm_b.run_end_ts AS TIMESTAMP)) - unix_timestamp(CAST(prm_b.run_start_ts AS TIMESTAMP))) AS BIGINT) AS latency_seconds,
            ROW_NUMBER() OVER (
                PARTITION BY prm_b.pipeline_name, prm_b.run_id
                ORDER BY CAST(prm_b.run_end_ts AS TIMESTAMP) DESC, CAST(prm_b.run_start_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM pipeline_run_metrics_bronze prm_b
    )
    SELECT
        pipeline_name,
        run_id,
        run_start_ts,
        run_end_ts,
        run_status,
        records_read,
        records_written,
        latency_seconds
    FROM dedup
    WHERE rn = 1
    """
)
pipeline_run_metrics_silver_df.createOrReplaceTempView("pipeline_run_metrics_silver")

pipeline_run_metrics_silver_out = f"{TARGET_PATH}/pipeline_run_metrics_silver.csv"
(
    pipeline_run_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(pipeline_run_metrics_silver_out)
)

# ============================================================
# Target: silver.data_quality_results_silver
# ============================================================
data_quality_results_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            CAST(dqr_b.dataset_name AS STRING) AS dataset_name,
            CAST(dqr_b.check_ts AS TIMESTAMP) AS check_ts,
            CAST(dqr_b.check_name AS STRING) AS check_name,
            CAST(dqr_b.check_status AS STRING) AS check_status,
            CAST(dqr_b.quality_score_percent AS DECIMAL(38,10)) AS quality_score_percent,
            CAST(dqr_b.failed_record_count AS BIGINT) AS failed_record_count,
            CAST(dqr_b.total_record_count AS BIGINT) AS total_record_count,
            ROW_NUMBER() OVER (
                PARTITION BY dqr_b.dataset_name, dqr_b.check_name, CAST(dqr_b.check_ts AS TIMESTAMP)
                ORDER BY CAST(dqr_b.check_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM data_quality_results_bronze dqr_b
    )
    SELECT
        dataset_name,
        check_ts,
        check_name,
        check_status,
        quality_score_percent,
        failed_record_count,
        total_record_count
    FROM dedup
    WHERE rn = 1
    """
)
data_quality_results_silver_df.createOrReplaceTempView("data_quality_results_silver")

data_quality_results_silver_out = f"{TARGET_PATH}/data_quality_results_silver.csv"
(
    data_quality_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(data_quality_results_silver_out)
)

# ============================================================
# Target: silver.data_catalog_assets_silver
# ============================================================
data_catalog_assets_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            CAST(dca_b.asset_id AS STRING) AS asset_id,
            CAST(dca_b.asset_name AS STRING) AS asset_name,
            CAST(dca_b.asset_type AS STRING) AS asset_type,
            CAST(dca_b.storage_layer AS STRING) AS storage_layer,
            CAST(dca_b.database_name AS STRING) AS database_name,
            CAST(dca_b.schema_name AS STRING) AS schema_name,
            CAST(dca_b.table_name AS STRING) AS table_name,
            CAST(dca_b.owner AS STRING) AS owner,
            CAST(dca_b.sensitivity_classification AS STRING) AS sensitivity_classification,
            CAST(dca_b.retention_policy_days AS INT) AS retention_policy_days,
            CAST(dca_b.is_pii AS BOOLEAN) AS is_pii,
            CAST(dca_b.effective_from_ts AS TIMESTAMP) AS effective_from_ts,
            CAST(dca_b.effective_to_ts AS TIMESTAMP) AS effective_to_ts,
            ROW_NUMBER() OVER (
                PARTITION BY dca_b.asset_id
                ORDER BY CAST(dca_b.effective_from_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM data_catalog_assets_bronze dca_b
    )
    SELECT
        asset_id,
        asset_name,
        asset_type,
        storage_layer,
        database_name,
        schema_name,
        table_name,
        owner,
        sensitivity_classification,
        retention_policy_days,
        is_pii,
        effective_from_ts,
        effective_to_ts
    FROM dedup
    WHERE rn = 1
    """
)
data_catalog_assets_silver_df.createOrReplaceTempView("data_catalog_assets_silver")

data_catalog_assets_silver_out = f"{TARGET_PATH}/data_catalog_assets_silver.csv"
(
    data_catalog_assets_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(data_catalog_assets_silver_out)
)

# ============================================================
# Target: silver.data_access_audit_silver
# ============================================================
data_access_audit_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            CAST(daa_b.access_ts AS TIMESTAMP) AS access_ts,
            CAST(daa_b.principal_id AS STRING) AS principal_id,
            CAST(daa_b.principal_type AS STRING) AS principal_type,
            CAST(daa_b.asset_id AS STRING) AS asset_id,
            CAST(daa_b.action AS STRING) AS action,
            CAST(daa_b.access_method AS STRING) AS access_method,
            CAST(daa_b.is_authorized AS BOOLEAN) AS is_authorized,
            CAST(daa_b.decision_reason AS STRING) AS decision_reason,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CAST(daa_b.access_ts AS TIMESTAMP),
                    daa_b.principal_id,
                    daa_b.asset_id,
                    daa_b.action
                ORDER BY CAST(daa_b.access_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM data_access_audit_bronze daa_b
    )
    SELECT
        access_ts,
        principal_id,
        principal_type,
        asset_id,
        action,
        access_method,
        is_authorized,
        decision_reason
    FROM dedup
    WHERE rn = 1
    """
)
data_access_audit_silver_df.createOrReplaceTempView("data_access_audit_silver")

data_access_audit_silver_out = f"{TARGET_PATH}/data_access_audit_silver.csv"
(
    data_access_audit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(data_access_audit_silver_out)
)

# ============================================================
# Target: silver.sla_availability_daily_silver
# Source: silver.pipeline_run_metrics_silver
# ============================================================
sla_availability_daily_silver_df = spark.sql(
    """
    SELECT
        CAST(prm_s.run_start_ts AS DATE) AS sla_date,
        CAST(prm_s.pipeline_name AS STRING) AS pipeline_name,
        CAST(COUNT(prm_s.run_id) AS INT) AS expected_runs,
        CAST(SUM(CASE WHEN prm_s.run_status = 'SUCCESS' THEN 1 ELSE 0 END) AS INT) AS successful_runs,
        CAST(
            (SUM(CASE WHEN prm_s.run_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / COUNT(prm_s.run_id)
            AS DECIMAL(38,10)
        ) AS availability_rate_percent
    FROM pipeline_run_metrics_silver prm_s
    GROUP BY
        CAST(prm_s.run_start_ts AS DATE),
        CAST(prm_s.pipeline_name AS STRING)
    """
)
sla_availability_daily_silver_df.createOrReplaceTempView("sla_availability_daily_silver")

sla_availability_daily_silver_out = f"{TARGET_PATH}/sla_availability_daily_silver.csv"
(
    sla_availability_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sla_availability_daily_silver_out)
)

job.commit()
