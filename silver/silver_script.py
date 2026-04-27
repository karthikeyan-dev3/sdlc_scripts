import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# --------------------------------------------------------------------------------
# Read Source Tables (Bronze) from S3
# --------------------------------------------------------------------------------
irb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_runs_bronze.{FILE_FORMAT}/")
)
irb_df.createOrReplaceTempView("ingestion_runs_bronze")

prb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/pipeline_runs_bronze.{FILE_FORMAT}/")
)
prb_df.createOrReplaceTempView("pipeline_runs_bronze")

dqcb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_checks_bronze.{FILE_FORMAT}/")
)
dqcb_df.createOrReplaceTempView("data_quality_checks_bronze")

dscb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dataset_catalog_bronze.{FILE_FORMAT}/")
)
dscb_df.createOrReplaceTempView("dataset_catalog_bronze")

daab_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_access_audit_bronze.{FILE_FORMAT}/")
)
daab_df.createOrReplaceTempView("data_access_audit_bronze")

# --------------------------------------------------------------------------------
# Target: silver_ingestion_kpi
# --------------------------------------------------------------------------------
silver_ingestion_kpi_df = spark.sql(
    """
    WITH base AS (
      SELECT
        irb.dataset_id,
        irb.source_system,
        irb.ingestion_method,
        irb.load_type,
        CAST(COALESCE(irb.ingest_start_ts, irb.ingest_end_ts) AS DATE) AS event_date,
        irb.batch_id,
        irb.ingest_start_ts,
        irb.ingest_end_ts,
        ROUND((irb.ingest_end_ts - irb.ingest_start_ts)/60, 2) AS ingestion_latency_minutes,
        irb.records_ingested,
        irb.bytes_ingested,
        irb.ingestion_status,
        irb.last_updated_ts
      FROM ingestion_runs_bronze irb
    ),
    standardized AS (
      SELECT
        dataset_id,
        TRIM(source_system) AS source_system,
        TRIM(ingestion_method) AS ingestion_method,
        TRIM(load_type) AS load_type,
        event_date,
        batch_id,
        ingest_start_ts,
        ingest_end_ts,
        CAST(ingestion_latency_minutes AS DECIMAL(18,2)) AS ingestion_latency_minutes,
        CAST(records_ingested AS INT) AS records_ingested,
        CAST(bytes_ingested AS DECIMAL(18,2)) AS bytes_ingested,
        CASE
          WHEN UPPER(TRIM(ingestion_status)) IN ('SUCCESS','SUCCEEDED','COMPLETED') THEN 'SUCCESS'
          WHEN UPPER(TRIM(ingestion_status)) IN ('FAILED','FAIL','ERROR') THEN 'FAILED'
          WHEN UPPER(TRIM(ingestion_status)) IN ('RUNNING','IN_PROGRESS','STARTED') THEN 'RUNNING'
          ELSE 'UNKNOWN'
        END AS ingestion_status,
        last_updated_ts
      FROM base
    ),
    deduped AS (
      SELECT
        dataset_id,
        source_system,
        ingestion_method,
        load_type,
        event_date,
        batch_id,
        ingest_start_ts,
        ingest_end_ts,
        ingestion_latency_minutes,
        records_ingested,
        bytes_ingested,
        ingestion_status,
        ROW_NUMBER() OVER (
          PARTITION BY dataset_id, batch_id, ingest_start_ts
          ORDER BY COALESCE(last_updated_ts, ingest_end_ts) DESC
        ) AS rn
      FROM standardized
    )
    SELECT
      dataset_id,
      source_system,
      ingestion_method,
      load_type,
      event_date,
      batch_id,
      ingest_start_ts,
      ingest_end_ts,
      ingestion_latency_minutes,
      records_ingested,
      bytes_ingested,
      ingestion_status
    FROM deduped
    WHERE rn = 1
    """
)

(
    silver_ingestion_kpi_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_ingestion_kpi.csv")
)

# --------------------------------------------------------------------------------
# Target: silver_pipeline_run
# --------------------------------------------------------------------------------
silver_pipeline_run_df = spark.sql(
    """
    WITH base AS (
      SELECT
        prb.pipeline_id,
        prb.pipeline_name,
        prb.orchestrator_tool,
        prb.run_id,
        prb.trigger_type,
        prb.run_start_ts,
        prb.run_end_ts,
        prb.run_status,
        prb.error_code,
        prb.error_message,
        prb.records_processed,
        prb.sla_minutes,
        CASE
          WHEN prb.run_end_ts IS NOT NULL AND prb.run_start_ts IS NOT NULL AND prb.sla_minutes IS NOT NULL
            THEN ((prb.run_end_ts - prb.run_start_ts)/60.0) <= prb.sla_minutes
          ELSE NULL
        END AS sla_met_flag,
        prb.updated_ts
      FROM pipeline_runs_bronze prb
    ),
    standardized AS (
      SELECT
        pipeline_id,
        pipeline_name,
        CASE
          WHEN UPPER(TRIM(orchestrator_tool)) IN ('AIRFLOW') THEN 'AIRFLOW'
          WHEN UPPER(TRIM(orchestrator_tool)) IN ('GLUE','AWS GLUE') THEN 'GLUE'
          WHEN UPPER(TRIM(orchestrator_tool)) IN ('SPARK') THEN 'SPARK'
          WHEN UPPER(TRIM(orchestrator_tool)) IN ('DBT') THEN 'DBT'
          WHEN orchestrator_tool IS NULL OR TRIM(orchestrator_tool) = '' THEN 'UNKNOWN'
          ELSE 'OTHER'
        END AS orchestrator_tool,
        run_id,
        trigger_type,
        run_start_ts,
        run_end_ts,
        CASE
          WHEN UPPER(TRIM(run_status)) IN ('SUCCESS','SUCCEEDED','COMPLETED') THEN 'SUCCESS'
          WHEN UPPER(TRIM(run_status)) IN ('FAILED','FAIL','ERROR') THEN 'FAILED'
          WHEN UPPER(TRIM(run_status)) IN ('RUNNING','IN_PROGRESS','STARTED') THEN 'RUNNING'
          WHEN UPPER(TRIM(run_status)) IN ('CANCELLED','CANCELED','ABORTED') THEN 'CANCELLED'
          ELSE 'UNKNOWN'
        END AS run_status,
        error_code,
        error_message,
        CAST(records_processed AS INT) AS records_processed,
        CAST(sla_minutes AS INT) AS sla_minutes,
        CAST(sla_met_flag AS BOOLEAN) AS sla_met_flag,
        updated_ts
      FROM base
    ),
    deduped AS (
      SELECT
        pipeline_id,
        pipeline_name,
        orchestrator_tool,
        run_id,
        trigger_type,
        run_start_ts,
        run_end_ts,
        run_status,
        error_code,
        error_message,
        records_processed,
        sla_minutes,
        sla_met_flag,
        ROW_NUMBER() OVER (
          PARTITION BY pipeline_id, run_id
          ORDER BY COALESCE(updated_ts, run_end_ts) DESC
        ) AS rn
      FROM standardized
    )
    SELECT
      pipeline_id,
      pipeline_name,
      orchestrator_tool,
      run_id,
      trigger_type,
      run_start_ts,
      run_end_ts,
      run_status,
      error_code,
      error_message,
      records_processed,
      sla_minutes,
      sla_met_flag
    FROM deduped
    WHERE rn = 1
    """
)

(
    silver_pipeline_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_pipeline_run.csv")
)

# --------------------------------------------------------------------------------
# Target: silver_data_quality_score
# --------------------------------------------------------------------------------
silver_data_quality_score_df = spark.sql(
    """
    WITH base AS (
      SELECT
        dqcb.dataset_id,
        dqcb.check_suite_id,
        dqcb.check_run_id,
        CAST(COALESCE(dqcb.check_start_ts, dqcb.check_end_ts) AS DATE) AS check_date,
        dqcb.check_start_ts,
        dqcb.check_end_ts,
        dqcb.rule_id,
        CASE
          WHEN UPPER(TRIM(dqcb.result)) IN ('PASS','PASSED') THEN 'PASS'
          WHEN UPPER(TRIM(dqcb.result)) IN ('FAIL','FAILED') THEN 'FAIL'
          WHEN UPPER(TRIM(dqcb.result)) IN ('SKIP','SKIPPED') THEN 'SKIP'
          WHEN UPPER(TRIM(dqcb.result)) IN ('ERROR','ERR') THEN 'ERROR'
          ELSE 'ERROR'
        END AS result,
        CASE
          WHEN UPPER(TRIM(dqcb.severity)) IN ('HIGH','MEDIUM','LOW') THEN UPPER(TRIM(dqcb.severity))
          ELSE NULL
        END AS severity,
        dqcb.updated_ts
      FROM data_quality_checks_bronze dqcb
    ),
    deduped_events AS (
      SELECT
        dataset_id,
        check_suite_id,
        check_run_id,
        check_date,
        check_start_ts,
        check_end_ts,
        rule_id,
        result,
        severity,
        ROW_NUMBER() OVER (
          PARTITION BY dataset_id, check_run_id, rule_id
          ORDER BY COALESCE(updated_ts, check_end_ts) DESC
        ) AS rn
      FROM base
    ),
    filtered AS (
      SELECT
        dataset_id,
        check_suite_id,
        check_run_id,
        check_date,
        check_start_ts,
        check_end_ts,
        rule_id,
        result,
        severity
      FROM deduped_events
      WHERE rn = 1
    )
    SELECT
      dataset_id,
      check_suite_id,
      check_run_id,
      check_date,
      MIN(check_start_ts) AS check_start_ts,
      MAX(check_end_ts) AS check_end_ts,
      COUNT(rule_id) AS total_checks,
      SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) AS passed_checks,
      SUM(CASE WHEN result IN ('FAIL','ERROR') THEN 1 ELSE 0 END) AS failed_checks,
      ROUND(
        100.0 * SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) / NULLIF(COUNT(rule_id), 0),
        2
      ) AS quality_score_pct,
      SUM(CASE WHEN result IN ('FAIL','ERROR') AND severity = 'HIGH' THEN 1 ELSE 0 END) AS severity_high_fail_count,
      SUM(CASE WHEN result IN ('FAIL','ERROR') AND severity = 'MEDIUM' THEN 1 ELSE 0 END) AS severity_medium_fail_count,
      SUM(CASE WHEN result IN ('FAIL','ERROR') AND severity = 'LOW' THEN 1 ELSE 0 END) AS severity_low_fail_count
    FROM filtered
    GROUP BY
      dataset_id,
      check_suite_id,
      check_run_id,
      check_date
    """
)

(
    silver_data_quality_score_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_data_quality_score.csv")
)

# --------------------------------------------------------------------------------
# Target: silver_dataset_catalog
# --------------------------------------------------------------------------------
silver_dataset_catalog_df = spark.sql(
    """
    WITH base AS (
      SELECT
        dscb.dataset_id,
        dscb.dataset_name,
        CASE
          WHEN UPPER(TRIM(dscb.data_layer)) IN ('BRONZE','SILVER','GOLD') THEN UPPER(TRIM(dscb.data_layer))
          ELSE NULL
        END AS data_layer,
        TRIM(dscb.domain) AS domain,
        TRIM(dscb.owner_team) AS owner_team,
        dscb.description,
        dscb.source_system,
        dscb.ingestion_method,
        dscb.update_frequency,
        CAST(dscb.pii_flag AS BOOLEAN) AS pii_flag,
        CASE
          WHEN UPPER(TRIM(dscb.classification)) IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')
            THEN UPPER(TRIM(dscb.classification))
          ELSE NULL
        END AS classification,
        CAST(dscb.retention_days AS INT) AS retention_days,
        dscb.schema_version,
        dscb.cataloged_ts,
        CAST(COALESCE(dscb.active_flag, true) AS BOOLEAN) AS active_flag,
        dscb.updated_ts
      FROM dataset_catalog_bronze dscb
    ),
    deduped AS (
      SELECT
        dataset_id,
        dataset_name,
        data_layer,
        domain,
        owner_team,
        description,
        source_system,
        ingestion_method,
        update_frequency,
        pii_flag,
        classification,
        retention_days,
        schema_version,
        cataloged_ts,
        active_flag,
        ROW_NUMBER() OVER (
          PARTITION BY dataset_id
          ORDER BY COALESCE(cataloged_ts, updated_ts) DESC
        ) AS rn
      FROM base
    )
    SELECT
      dataset_id,
      dataset_name,
      data_layer,
      domain,
      owner_team,
      description,
      source_system,
      ingestion_method,
      update_frequency,
      pii_flag,
      classification,
      retention_days,
      schema_version,
      cataloged_ts,
      active_flag
    FROM deduped
    WHERE rn = 1
    """
)

(
    silver_dataset_catalog_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_dataset_catalog.csv")
)

# --------------------------------------------------------------------------------
# Target: silver_data_access_audit
# --------------------------------------------------------------------------------
silver_data_access_audit_df = spark.sql(
    """
    WITH base AS (
      SELECT
        daab.request_id,
        daab.request_ts,
        daab.principal_id,
        daab.principal_type,
        daab.api_endpoint,
        daab.http_method,
        daab.dataset_id,
        daab.action,
        daab.authz_decision,
        daab.response_status_code,
        daab.response_time_ms,
        daab.updated_ts
      FROM data_access_audit_bronze daab
    ),
    standardized AS (
      SELECT
        request_id,
        request_ts,
        principal_id,
        CASE
          WHEN UPPER(TRIM(principal_type)) IN ('USER') THEN 'USER'
          WHEN UPPER(TRIM(principal_type)) IN ('SERVICE','SERVICE_ACCOUNT','APP') THEN 'SERVICE'
          WHEN UPPER(TRIM(principal_type)) IN ('ROLE') THEN 'ROLE'
          ELSE 'UNKNOWN'
        END AS principal_type,
        api_endpoint,
        CASE
          WHEN http_method IS NULL OR TRIM(http_method) = '' THEN NULL
          ELSE UPPER(TRIM(http_method))
        END AS http_method,
        dataset_id,
        CASE
          WHEN UPPER(TRIM(action)) IN ('READ','WRITE','DELETE','ADMIN') THEN UPPER(TRIM(action))
          WHEN action IS NULL OR TRIM(action) = '' THEN 'OTHER'
          ELSE 'OTHER'
        END AS action,
        CASE
          WHEN UPPER(TRIM(authz_decision)) IN ('ALLOW','ALLOWED') THEN 'ALLOW'
          WHEN UPPER(TRIM(authz_decision)) IN ('DENY','DENIED') THEN 'DENY'
          ELSE 'UNKNOWN'
        END AS authz_decision,
        CAST(response_status_code AS INT) AS response_status_code,
        CAST(response_time_ms AS INT) AS response_time_ms,
        updated_ts
      FROM base
    ),
    deduped AS (
      SELECT
        request_id,
        request_ts,
        principal_id,
        principal_type,
        api_endpoint,
        http_method,
        dataset_id,
        action,
        authz_decision,
        response_status_code,
        response_time_ms,
        ROW_NUMBER() OVER (
          PARTITION BY
            COALESCE(
              request_id,
              CONCAT_WS(
                '||',
                COALESCE(principal_id, ''),
                COALESCE(CAST(request_ts AS STRING), ''),
                COALESCE(api_endpoint, ''),
                COALESCE(http_method, ''),
                COALESCE(dataset_id, '')
              )
            )
          ORDER BY updated_ts DESC
        ) AS rn
      FROM standardized
    )
    SELECT
      request_id,
      request_ts,
      principal_id,
      principal_type,
      api_endpoint,
      http_method,
      dataset_id,
      action,
      authz_decision,
      response_status_code,
      response_time_ms
    FROM deduped
    WHERE rn = 1
    """
)

(
    silver_data_access_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_data_access_audit.csv")
)

job.commit()