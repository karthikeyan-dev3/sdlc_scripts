import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ============================================================
# 1) pipeline_run_silver
# ============================================================
pipeline_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/pipeline_runs_bronze.{FILE_FORMAT}/")
)
pipeline_runs_bronze_df.createOrReplaceTempView("pipeline_runs_bronze")

pipeline_run_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(prb.pipeline_run_id) AS STRING) AS pipeline_run_id,
        CAST(TRIM(prb.pipeline_id) AS STRING) AS pipeline_id,
        CAST(TRIM(prb.orchestrator_tool) AS STRING) AS orchestrator_tool,
        CAST(UPPER(TRIM(prb.run_status)) AS STRING) AS run_status,
        CAST(prb.run_start_ts AS TIMESTAMP) AS run_start_ts,
        CAST(prb.run_end_ts AS TIMESTAMP) AS run_end_ts,
        CAST(prb.records_processed_count AS INT) AS records_processed_count,
        CAST(TRIM(prb.error_message) AS STRING) AS error_message,
        CAST(prb.data_freshness_lag_seconds AS INT) AS data_freshness_lag_seconds
      FROM pipeline_runs_bronze prb
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY pipeline_run_id
          ORDER BY run_end_ts DESC NULLS LAST, run_start_ts DESC NULLS LAST
        ) AS rn
      FROM base
      WHERE pipeline_run_id IS NOT NULL AND TRIM(pipeline_run_id) <> ''
    )
    SELECT
      pipeline_run_id,
      pipeline_id,
      orchestrator_tool,
      run_status,
      run_start_ts,
      run_end_ts,
      records_processed_count,
      error_message,
      data_freshness_lag_seconds
    FROM dedup
    WHERE rn = 1
    """
)
pipeline_run_silver_df.createOrReplaceTempView("pipeline_run_silver")

(
    pipeline_run_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pipeline_run_silver.csv")
)

# ============================================================
# 2) dataset_silver
# ============================================================
datasets_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/datasets_bronze.{FILE_FORMAT}/")
)
datasets_bronze_df.createOrReplaceTempView("datasets_bronze")

dataset_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(db.dataset_id) AS STRING) AS dataset_id,
        CAST(TRIM(db.dataset_name) AS STRING) AS dataset_name,
        CAST(LOWER(TRIM(db.layer)) AS STRING) AS layer,
        CAST(LOWER(TRIM(db.source_system)) AS STRING) AS source_system,
        CAST(LOWER(TRIM(db.domain)) AS STRING) AS domain,
        CAST(TRIM(db.owner_role) AS STRING) AS owner_role,
        CAST(db.created_ts AS TIMESTAMP) AS created_ts,
        CAST(COALESCE(db.is_active, TRUE) AS BOOLEAN) AS is_active
      FROM datasets_bronze db
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY dataset_id
          ORDER BY created_ts DESC NULLS LAST
        ) AS rn
      FROM base
      WHERE dataset_id IS NOT NULL AND TRIM(dataset_id) <> ''
    )
    SELECT
      dataset_id,
      dataset_name,
      layer,
      source_system,
      domain,
      owner_role,
      created_ts,
      is_active
    FROM dedup
    WHERE rn = 1
    """
)
dataset_silver_df.createOrReplaceTempView("dataset_silver")

(
    dataset_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dataset_silver.csv")
)

# ============================================================
# 3) dataset_lineage_silver
# ============================================================
dataset_lineage_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dataset_lineage_bronze.{FILE_FORMAT}/")
)
dataset_lineage_bronze_df.createOrReplaceTempView("dataset_lineage_bronze")

dataset_lineage_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(dlb.upstream_dataset_id) AS STRING) AS upstream_dataset_id,
        CAST(TRIM(dlb.downstream_dataset_id) AS STRING) AS downstream_dataset_id,
        CAST(LOWER(TRIM(dlb.lineage_type)) AS STRING) AS lineage_type,
        CAST(dlb.effective_start_ts AS TIMESTAMP) AS effective_start_ts,
        CAST(dlb.effective_end_ts AS TIMESTAMP) AS effective_end_ts
      FROM dataset_lineage_bronze dlb
      INNER JOIN dataset_silver uds
        ON CAST(TRIM(dlb.upstream_dataset_id) AS STRING) = uds.dataset_id
      INNER JOIN dataset_silver dds
        ON CAST(TRIM(dlb.downstream_dataset_id) AS STRING) = dds.dataset_id
    ),
    filtered AS (
      SELECT *
      FROM base
      WHERE upstream_dataset_id IS NOT NULL AND TRIM(upstream_dataset_id) <> ''
        AND downstream_dataset_id IS NOT NULL AND TRIM(downstream_dataset_id) <> ''
        AND (effective_end_ts IS NULL OR effective_start_ts IS NULL OR effective_end_ts >= effective_start_ts)
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY upstream_dataset_id, downstream_dataset_id, lineage_type, effective_start_ts, effective_end_ts
          ORDER BY effective_end_ts DESC NULLS LAST, effective_start_ts DESC NULLS LAST
        ) AS rn
      FROM filtered
    )
    SELECT
      upstream_dataset_id,
      downstream_dataset_id,
      lineage_type,
      effective_start_ts,
      effective_end_ts
    FROM dedup
    WHERE rn = 1
    """
)
dataset_lineage_silver_df.createOrReplaceTempView("dataset_lineage_silver")

(
    dataset_lineage_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dataset_lineage_silver.csv")
)

# ============================================================
# 4) data_quality_check_result_silver
# ============================================================
dq_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_check_results_bronze.{FILE_FORMAT}/")
)
dq_results_bronze_df.createOrReplaceTempView("data_quality_check_results_bronze")

data_quality_check_result_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(dqb.dq_result_id) AS STRING) AS dq_result_id,
        CAST(TRIM(dqb.dataset_id) AS STRING) AS dataset_id,
        CAST(TRIM(dqb.pipeline_run_id) AS STRING) AS pipeline_run_id,
        CAST(TRIM(dqb.check_name) AS STRING) AS check_name,
        CAST(LOWER(TRIM(dqb.check_type)) AS STRING) AS check_type,
        CAST(UPPER(TRIM(dqb.severity)) AS STRING) AS severity,
        CAST(dqb.threshold_value AS DECIMAL(38, 10)) AS threshold_value,
        CAST(dqb.actual_value AS DECIMAL(38, 10)) AS actual_value,
        CAST(UPPER(TRIM(dqb.check_status)) AS STRING) AS check_status,
        CAST(dqb.evaluated_ts AS TIMESTAMP) AS evaluated_ts
      FROM data_quality_check_results_bronze dqb
      INNER JOIN dataset_silver ds
        ON CAST(TRIM(dqb.dataset_id) AS STRING) = ds.dataset_id
      INNER JOIN pipeline_run_silver prs
        ON CAST(TRIM(dqb.pipeline_run_id) AS STRING) = prs.pipeline_run_id
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY dq_result_id
          ORDER BY evaluated_ts DESC NULLS LAST
        ) AS rn
      FROM base
      WHERE dq_result_id IS NOT NULL AND TRIM(dq_result_id) <> ''
    )
    SELECT
      dq_result_id,
      dataset_id,
      pipeline_run_id,
      check_name,
      check_type,
      severity,
      threshold_value,
      actual_value,
      check_status,
      evaluated_ts
    FROM dedup
    WHERE rn = 1
    """
)
data_quality_check_result_silver_df.createOrReplaceTempView("data_quality_check_result_silver")

(
    data_quality_check_result_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_check_result_silver.csv")
)

# ============================================================
# 5) governance_policy_silver
# ============================================================
governance_policies_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/governance_policies_bronze.{FILE_FORMAT}/")
)
governance_policies_bronze_df.createOrReplaceTempView("governance_policies_bronze")

governance_policy_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(gpb.policy_id) AS STRING) AS policy_id,
        CAST(TRIM(gpb.policy_name) AS STRING) AS policy_name,
        CAST(LOWER(TRIM(gpb.policy_type)) AS STRING) AS policy_type,
        CAST(LOWER(TRIM(gpb.applies_to_layer)) AS STRING) AS applies_to_layer,
        CAST(LOWER(TRIM(gpb.applies_to_domain)) AS STRING) AS applies_to_domain,
        CAST(TRIM(gpb.required_control) AS STRING) AS required_control,
        CAST(gpb.effective_start_ts AS TIMESTAMP) AS effective_start_ts,
        CAST(gpb.effective_end_ts AS TIMESTAMP) AS effective_end_ts,
        CAST(TRIM(gpb.policy_owner_role) AS STRING) AS policy_owner_role
      FROM governance_policies_bronze gpb
    ),
    filtered AS (
      SELECT *
      FROM base
      WHERE policy_id IS NOT NULL AND TRIM(policy_id) <> ''
        AND (effective_end_ts IS NULL OR effective_start_ts IS NULL OR effective_end_ts >= effective_start_ts)
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY policy_id
          ORDER BY effective_start_ts DESC NULLS LAST, effective_end_ts DESC NULLS LAST
        ) AS rn
      FROM filtered
    )
    SELECT
      policy_id,
      policy_name,
      policy_type,
      applies_to_layer,
      applies_to_domain,
      required_control,
      effective_start_ts,
      effective_end_ts,
      policy_owner_role
    FROM dedup
    WHERE rn = 1
    """
)
governance_policy_silver_df.createOrReplaceTempView("governance_policy_silver")

(
    governance_policy_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/governance_policy_silver.csv")
)

# ============================================================
# 6) access_audit_event_silver
# ============================================================
access_audit_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/access_audit_events_bronze.{FILE_FORMAT}/")
)
access_audit_events_bronze_df.createOrReplaceTempView("access_audit_events_bronze")

access_audit_event_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(aab.audit_event_id) AS STRING) AS audit_event_id,
        CAST(aab.event_ts AS TIMESTAMP) AS event_ts,
        CAST(TRIM(aab.actor_role) AS STRING) AS actor_role,
        CAST(LOWER(TRIM(aab.action_type)) AS STRING) AS action_type,
        CAST(TRIM(aab.dataset_id) AS STRING) AS dataset_id,
        CAST(LOWER(TRIM(aab.access_channel)) AS STRING) AS access_channel,
        CAST(UPPER(TRIM(aab.access_result)) AS STRING) AS access_result,
        CAST(TRIM(aab.policy_id) AS STRING) AS policy_id
      FROM access_audit_events_bronze aab
      INNER JOIN dataset_silver ds
        ON CAST(TRIM(aab.dataset_id) AS STRING) = ds.dataset_id
      LEFT JOIN governance_policy_silver gps
        ON CAST(TRIM(aab.policy_id) AS STRING) = gps.policy_id
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY audit_event_id
          ORDER BY event_ts DESC NULLS LAST
        ) AS rn
      FROM base
      WHERE audit_event_id IS NOT NULL AND TRIM(audit_event_id) <> ''
    )
    SELECT
      audit_event_id,
      event_ts,
      actor_role,
      action_type,
      dataset_id,
      access_channel,
      access_result,
      policy_id
    FROM dedup
    WHERE rn = 1
    """
)
access_audit_event_silver_df.createOrReplaceTempView("access_audit_event_silver")

(
    access_audit_event_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/access_audit_event_silver.csv")
)

# ============================================================
# 7) system_availability_sla_daily_silver
# ============================================================
system_availability_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/system_availability_sla_daily_bronze.{FILE_FORMAT}/")
)
system_availability_bronze_df.createOrReplaceTempView("system_availability_sla_daily_bronze")

system_availability_sla_daily_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(sab.sla_date AS DATE) AS sla_date,
        CAST(TRIM(sab.service_name) AS STRING) AS service_name,
        CAST(COALESCE(sab.uptime_seconds, 0) AS INT) AS uptime_seconds,
        CAST(COALESCE(sab.downtime_seconds, 0) AS INT) AS downtime_seconds,
        CAST(sab.availability_pct AS DECIMAL(38, 10)) AS availability_pct
      FROM system_availability_sla_daily_bronze sab
    ),
    cleansed AS (
      SELECT
        sla_date,
        service_name,
        CASE WHEN uptime_seconds < 0 THEN 0 ELSE uptime_seconds END AS uptime_seconds,
        CASE WHEN downtime_seconds < 0 THEN 0 ELSE downtime_seconds END AS downtime_seconds,
        availability_pct
      FROM base
      WHERE sla_date IS NOT NULL
        AND service_name IS NOT NULL AND TRIM(service_name) <> ''
    ),
    recompute AS (
      SELECT
        sla_date,
        service_name,
        uptime_seconds,
        downtime_seconds,
        CAST(
          COALESCE(
            availability_pct,
            CASE
              WHEN (uptime_seconds + downtime_seconds) = 0 THEN CAST(NULL AS DECIMAL(38, 10))
              ELSE CAST(uptime_seconds AS DECIMAL(38, 10)) / CAST((uptime_seconds + downtime_seconds) AS DECIMAL(38, 10))
            END
          ) AS DECIMAL(38, 10)
        ) AS availability_pct
      FROM cleansed
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY sla_date, service_name
          ORDER BY availability_pct DESC NULLS LAST
        ) AS rn
      FROM recompute
    )
    SELECT
      sla_date,
      service_name,
      uptime_seconds,
      downtime_seconds,
      availability_pct
    FROM dedup
    WHERE rn = 1
    """
)
system_availability_sla_daily_silver_df.createOrReplaceTempView("system_availability_sla_daily_silver")

(
    system_availability_sla_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/system_availability_sla_daily_silver.csv")
)

# ============================================================
# 8) ingestion_feed_silver
# ============================================================
ingestion_feeds_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_feeds_bronze.{FILE_FORMAT}/")
)
ingestion_feeds_bronze_df.createOrReplaceTempView("ingestion_feeds_bronze")

ingestion_feed_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(ifb.feed_id) AS STRING) AS feed_id,
        CAST(TRIM(ifb.feed_name) AS STRING) AS feed_name,
        CAST(LOWER(TRIM(ifb.ingestion_method)) AS STRING) AS ingestion_method,
        CAST(LOWER(TRIM(ifb.source_system)) AS STRING) AS source_system,
        CAST(TRIM(ifb.target_dataset_id) AS STRING) AS target_dataset_id,
        CAST(ifb.expected_frequency_minutes AS INT) AS expected_frequency_minutes,
        CAST(ifb.last_success_ts AS TIMESTAMP) AS last_success_ts,
        CAST(UPPER(TRIM(ifb.current_status)) AS STRING) AS current_status
      FROM ingestion_feeds_bronze ifb
      INNER JOIN dataset_silver ds
        ON CAST(TRIM(ifb.target_dataset_id) AS STRING) = ds.dataset_id
    ),
    cleansed AS (
      SELECT
        feed_id,
        feed_name,
        ingestion_method,
        source_system,
        target_dataset_id,
        CASE
          WHEN expected_frequency_minutes IS NULL THEN NULL
          WHEN expected_frequency_minutes < 0 THEN NULL
          ELSE expected_frequency_minutes
        END AS expected_frequency_minutes,
        last_success_ts,
        current_status
      FROM base
      WHERE feed_id IS NOT NULL AND TRIM(feed_id) <> ''
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY feed_id
          ORDER BY last_success_ts DESC NULLS LAST
        ) AS rn
      FROM cleansed
    )
    SELECT
      feed_id,
      feed_name,
      ingestion_method,
      source_system,
      target_dataset_id,
      expected_frequency_minutes,
      last_success_ts,
      current_status
    FROM dedup
    WHERE rn = 1
    """
)
ingestion_feed_silver_df.createOrReplaceTempView("ingestion_feed_silver")

(
    ingestion_feed_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestion_feed_silver.csv")
)

job.commit()