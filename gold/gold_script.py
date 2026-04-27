import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Read Source Tables (S3)
# ------------------------------------------------------------------------------------
pipeline_run_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/pipeline_run_metrics_silver.{FILE_FORMAT}/")
)
pipeline_run_metrics_silver_df.createOrReplaceTempView("pipeline_run_metrics_silver")

data_quality_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_results_silver.{FILE_FORMAT}/")
)
data_quality_results_silver_df.createOrReplaceTempView("data_quality_results_silver")

data_catalog_assets_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_catalog_assets_silver.{FILE_FORMAT}/")
)
data_catalog_assets_silver_df.createOrReplaceTempView("data_catalog_assets_silver")

data_access_audit_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_access_audit_silver.{FILE_FORMAT}/")
)
data_access_audit_silver_df.createOrReplaceTempView("data_access_audit_silver")

sla_availability_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sla_availability_daily_silver.{FILE_FORMAT}/")
)
sla_availability_daily_silver_df.createOrReplaceTempView("sla_availability_daily_silver")

# ------------------------------------------------------------------------------------
# Target: gold_pipeline_run_metrics
# ------------------------------------------------------------------------------------
gold_pipeline_run_metrics_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(prm_s.pipeline_name AS STRING)    AS pipeline_name,
            CAST(prm_s.run_id AS STRING)           AS run_id,
            CAST(prm_s.run_start_ts AS TIMESTAMP)  AS run_start_ts,
            CAST(prm_s.run_end_ts AS TIMESTAMP)    AS run_end_ts,
            CAST(prm_s.run_status AS STRING)       AS run_status,
            CAST(prm_s.records_read AS BIGINT)     AS records_read,
            CAST(prm_s.records_written AS BIGINT)  AS records_written,
            CAST(prm_s.latency_seconds AS BIGINT)  AS latency_seconds
        FROM pipeline_run_metrics_silver prm_s
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY pipeline_name, run_id
                ORDER BY run_end_ts DESC
            ) AS rn
        FROM base
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

(
    gold_pipeline_run_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_pipeline_run_metrics.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_data_quality_results
# ------------------------------------------------------------------------------------
gold_data_quality_results_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(dqr_s.dataset_name AS STRING)            AS dataset_name,
            CAST(dqr_s.check_ts AS TIMESTAMP)             AS check_ts,
            CAST(dqr_s.check_name AS STRING)              AS check_name,
            CAST(dqr_s.check_status AS STRING)            AS check_status,
            CAST(dqr_s.quality_score_percent AS DECIMAL(38,18)) AS quality_score_percent,
            CAST(dqr_s.failed_record_count AS BIGINT)     AS failed_record_count,
            CAST(dqr_s.total_record_count AS BIGINT)      AS total_record_count
        FROM data_quality_results_silver dqr_s
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY dataset_name, check_ts, check_name
                ORDER BY check_ts DESC
            ) AS rn
        FROM base
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

(
    gold_data_quality_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_results.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_data_catalog_assets
# ------------------------------------------------------------------------------------
gold_data_catalog_assets_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(dca_s.asset_id AS STRING)                     AS asset_id,
            CAST(dca_s.asset_name AS STRING)                   AS asset_name,
            CAST(dca_s.asset_type AS STRING)                   AS asset_type,
            CAST(dca_s.storage_layer AS STRING)                AS storage_layer,
            CAST(dca_s.database_name AS STRING)                AS database_name,
            CAST(dca_s.schema_name AS STRING)                  AS schema_name,
            CAST(dca_s.table_name AS STRING)                   AS table_name,
            CAST(dca_s.owner AS STRING)                        AS owner,
            CAST(dca_s.sensitivity_classification AS STRING)   AS sensitivity_classification,
            CAST(dca_s.retention_policy_days AS INT)           AS retention_policy_days,
            CAST(dca_s.is_pii AS BOOLEAN)                      AS is_pii,
            CAST(dca_s.effective_from_ts AS TIMESTAMP)         AS effective_from_ts,
            CAST(dca_s.effective_to_ts AS TIMESTAMP)           AS effective_to_ts
        FROM data_catalog_assets_silver dca_s
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY asset_id, effective_from_ts
                ORDER BY COALESCE(effective_to_ts, TIMESTAMP('2999-12-31 00:00:00')) DESC
            ) AS rn
        FROM base
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

(
    gold_data_catalog_assets_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_catalog_assets.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_data_access_audit
# ------------------------------------------------------------------------------------
gold_data_access_audit_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(daa_s.access_ts AS TIMESTAMP)     AS access_ts,
            CAST(daa_s.principal_id AS STRING)     AS principal_id,
            CAST(daa_s.principal_type AS STRING)   AS principal_type,
            CAST(daa_s.asset_id AS STRING)         AS asset_id,
            CAST(daa_s.action AS STRING)           AS action,
            CAST(daa_s.access_method AS STRING)    AS access_method,
            CAST(daa_s.is_authorized AS BOOLEAN)   AS is_authorized,
            CAST(daa_s.decision_reason AS STRING)  AS decision_reason
        FROM data_access_audit_silver daa_s
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY access_ts, principal_id, asset_id, action
                ORDER BY access_ts DESC
            ) AS rn
        FROM base
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

(
    gold_data_access_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_access_audit.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_sla_availability_daily
# ------------------------------------------------------------------------------------
gold_sla_availability_daily_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sad_s.sla_date AS DATE)                     AS sla_date,
            CAST(sad_s.pipeline_name AS STRING)              AS pipeline_name,
            CAST(sad_s.expected_runs AS INT)                 AS expected_runs,
            CAST(sad_s.successful_runs AS INT)               AS successful_runs,
            CAST(sad_s.availability_rate_percent AS DECIMAL(38,18)) AS availability_rate_percent
        FROM sla_availability_daily_silver sad_s
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY sla_date, pipeline_name
                ORDER BY sla_date DESC
            ) AS rn
        FROM base
    )
    SELECT
        sla_date,
        pipeline_name,
        expected_runs,
        successful_runs,
        availability_rate_percent
    FROM dedup
    WHERE rn = 1
    """
)

(
    gold_sla_availability_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sla_availability_daily.csv")
)