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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
pipeline_run_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/pipeline_run_silver.{FILE_FORMAT}/")
)

dataset_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dataset_silver.{FILE_FORMAT}/")
)

dataset_lineage_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dataset_lineage_silver.{FILE_FORMAT}/")
)

data_quality_check_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_check_result_silver.{FILE_FORMAT}/")
)

access_audit_event_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/access_audit_event_silver.{FILE_FORMAT}/")
)

governance_policy_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/governance_policy_silver.{FILE_FORMAT}/")
)

system_availability_sla_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/system_availability_sla_daily_silver.{FILE_FORMAT}/")
)

ingestion_feed_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_feed_silver.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
pipeline_run_silver_df.createOrReplaceTempView("pipeline_run_silver")
dataset_silver_df.createOrReplaceTempView("dataset_silver")
dataset_lineage_silver_df.createOrReplaceTempView("dataset_lineage_silver")
data_quality_check_result_silver_df.createOrReplaceTempView("data_quality_check_result_silver")
access_audit_event_silver_df.createOrReplaceTempView("access_audit_event_silver")
governance_policy_silver_df.createOrReplaceTempView("governance_policy_silver")
system_availability_sla_daily_silver_df.createOrReplaceTempView("system_availability_sla_daily_silver")
ingestion_feed_silver_df.createOrReplaceTempView("ingestion_feed_silver")

# ============================================================
# TARGET: pipeline_run_gold
# ============================================================
pipeline_run_gold_df = spark.sql(
    """
    SELECT
        CAST(prs.pipeline_run_id AS STRING)                AS pipeline_run_id,
        CAST(prs.pipeline_id AS STRING)                    AS pipeline_id,
        CAST(prs.orchestrator_tool AS STRING)              AS orchestrator_tool,
        CAST(prs.run_status AS STRING)                     AS run_status,
        CAST(prs.run_start_ts AS TIMESTAMP)                AS run_start_ts,
        CAST(prs.run_end_ts AS TIMESTAMP)                  AS run_end_ts,
        CAST(prs.records_processed_count AS INT)           AS records_processed_count,
        CAST(prs.error_message AS STRING)                  AS error_message,
        CAST(prs.data_freshness_lag_seconds AS INT)        AS data_freshness_lag_seconds
    FROM pipeline_run_silver prs
    """
)

(
    pipeline_run_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pipeline_run_gold.csv")
)

# ============================================================
# TARGET: dataset_gold
# ============================================================
dataset_gold_df = spark.sql(
    """
    SELECT
        CAST(ds.dataset_id AS STRING)        AS dataset_id,
        CAST(ds.dataset_name AS STRING)      AS dataset_name,
        CAST(ds.layer AS STRING)             AS layer,
        CAST(ds.source_system AS STRING)     AS source_system,
        CAST(ds.domain AS STRING)            AS domain,
        CAST(ds.owner_role AS STRING)        AS owner_role,
        CAST(ds.created_ts AS TIMESTAMP)     AS created_ts,
        CAST(ds.is_active AS BOOLEAN)        AS is_active
    FROM dataset_silver ds
    """
)

(
    dataset_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dataset_gold.csv")
)

# ============================================================
# TARGET: dataset_lineage_gold
# ============================================================
dataset_lineage_gold_df = spark.sql(
    """
    SELECT
        CAST(dls.upstream_dataset_id AS STRING)      AS upstream_dataset_id,
        CAST(dls.downstream_dataset_id AS STRING)    AS downstream_dataset_id,
        CAST(dls.lineage_type AS STRING)             AS lineage_type,
        CAST(dls.effective_start_ts AS TIMESTAMP)    AS effective_start_ts,
        CAST(dls.effective_end_ts AS TIMESTAMP)      AS effective_end_ts
    FROM dataset_lineage_silver dls
    INNER JOIN dataset_silver uds
        ON dls.upstream_dataset_id = uds.dataset_id
    INNER JOIN dataset_silver dds
        ON dls.downstream_dataset_id = dds.dataset_id
    """
)

(
    dataset_lineage_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dataset_lineage_gold.csv")
)

# ============================================================
# TARGET: data_quality_check_result_gold
# ============================================================
data_quality_check_result_gold_df = spark.sql(
    """
    SELECT
        CAST(dqrs.dq_result_id AS STRING)     AS dq_result_id,
        CAST(dqrs.dataset_id AS STRING)      AS dataset_id,
        CAST(dqrs.pipeline_run_id AS STRING) AS pipeline_run_id,
        CAST(dqrs.check_name AS STRING)      AS check_name,
        CAST(dqrs.check_type AS STRING)      AS check_type,
        CAST(dqrs.severity AS STRING)        AS severity,
        CAST(dqrs.threshold_value AS DECIMAL(38,18)) AS threshold_value,
        CAST(dqrs.actual_value AS DECIMAL(38,18))    AS actual_value,
        CAST(dqrs.check_status AS STRING)    AS check_status,
        CAST(dqrs.evaluated_ts AS TIMESTAMP) AS evaluated_ts
    FROM data_quality_check_result_silver dqrs
    INNER JOIN dataset_silver ds
        ON dqrs.dataset_id = ds.dataset_id
    INNER JOIN pipeline_run_silver prs
        ON dqrs.pipeline_run_id = prs.pipeline_run_id
    """
)

(
    data_quality_check_result_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_check_result_gold.csv")
)

# ============================================================
# TARGET: access_audit_event_gold
# ============================================================
access_audit_event_gold_df = spark.sql(
    """
    SELECT
        CAST(aaes.audit_event_id AS STRING)   AS audit_event_id,
        CAST(aaes.event_ts AS TIMESTAMP)      AS event_ts,
        CAST(aaes.actor_role AS STRING)       AS actor_role,
        CAST(aaes.action_type AS STRING)      AS action_type,
        CAST(aaes.dataset_id AS STRING)       AS dataset_id,
        CAST(aaes.access_channel AS STRING)   AS access_channel,
        CAST(aaes.access_result AS STRING)    AS access_result,
        CAST(aaes.policy_id AS STRING)        AS policy_id
    FROM access_audit_event_silver aaes
    INNER JOIN dataset_silver ds
        ON aaes.dataset_id = ds.dataset_id
    LEFT JOIN governance_policy_silver gps
        ON aaes.policy_id = gps.policy_id
    """
)

(
    access_audit_event_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/access_audit_event_gold.csv")
)

# ============================================================
# TARGET: governance_policy_gold
# ============================================================
governance_policy_gold_df = spark.sql(
    """
    SELECT
        CAST(gps.policy_id AS STRING)              AS policy_id,
        CAST(gps.policy_name AS STRING)            AS policy_name,
        CAST(gps.policy_type AS STRING)            AS policy_type,
        CAST(gps.applies_to_layer AS STRING)       AS applies_to_layer,
        CAST(gps.applies_to_domain AS STRING)      AS applies_to_domain,
        CAST(gps.required_control AS STRING)       AS required_control,
        CAST(gps.effective_start_ts AS TIMESTAMP)  AS effective_start_ts,
        CAST(gps.effective_end_ts AS TIMESTAMP)    AS effective_end_ts,
        CAST(gps.policy_owner_role AS STRING)      AS policy_owner_role
    FROM governance_policy_silver gps
    """
)

(
    governance_policy_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/governance_policy_gold.csv")
)

# ============================================================
# TARGET: system_availability_sla_daily_gold
# ============================================================
system_availability_sla_daily_gold_df = spark.sql(
    """
    SELECT
        CAST(sas.sla_date AS DATE)                 AS sla_date,
        CAST(sas.service_name AS STRING)           AS service_name,
        CAST(sas.uptime_seconds AS INT)            AS uptime_seconds,
        CAST(sas.downtime_seconds AS INT)          AS downtime_seconds,
        CAST(sas.availability_pct AS DECIMAL(38,18)) AS availability_pct
    FROM system_availability_sla_daily_silver sas
    """
)

(
    system_availability_sla_daily_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/system_availability_sla_daily_gold.csv")
)

# ============================================================
# TARGET: ingestion_feed_gold
# ============================================================
ingestion_feed_gold_df = spark.sql(
    """
    SELECT
        CAST(ifs.feed_id AS STRING)                  AS feed_id,
        CAST(ifs.feed_name AS STRING)                AS feed_name,
        CAST(ifs.ingestion_method AS STRING)         AS ingestion_method,
        CAST(ifs.source_system AS STRING)            AS source_system,
        CAST(ifs.target_dataset_id AS STRING)        AS target_dataset_id,
        CAST(ifs.expected_frequency_minutes AS INT)  AS expected_frequency_minutes,
        CAST(ifs.last_success_ts AS TIMESTAMP)       AS last_success_ts,
        CAST(ifs.current_status AS STRING)           AS current_status
    FROM ingestion_feed_silver ifs
    INNER JOIN dataset_silver ds
        ON ifs.target_dataset_id = ds.dataset_id
    """
)

(
    ingestion_feed_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestion_feed_gold.csv")
)

job.commit()
