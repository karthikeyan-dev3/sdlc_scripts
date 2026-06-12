import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Read source tables from S3 (CSV) and create temp views
# ---------------------------------------------------------------------------

silver_project_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_project.{FILE_FORMAT}/")
)
silver_project_df.createOrReplaceTempView("silver_project")

silver_sample_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sample.{FILE_FORMAT}/")
)
silver_sample_df.createOrReplaceTempView("silver_sample")

silver_instrument_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_instrument.{FILE_FORMAT}/")
)
silver_instrument_df.createOrReplaceTempView("silver_instrument")

silver_machine_project_map_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_machine_project_map.{FILE_FORMAT}/")
)
silver_machine_project_map_df.createOrReplaceTempView("silver_machine_project_map")

silver_experiment_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_experiment.{FILE_FORMAT}/")
)
silver_experiment_df.createOrReplaceTempView("silver_experiment")

silver_run_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_run.{FILE_FORMAT}/")
)
silver_run_df.createOrReplaceTempView("silver_run")

silver_experiment_observation_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_experiment_observation.{FILE_FORMAT}/")
)
silver_experiment_observation_df.createOrReplaceTempView("silver_experiment_observation")

silver_data_quality_check_result_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_data_quality_check_result.{FILE_FORMAT}/")
)
silver_data_quality_check_result_df.createOrReplaceTempView("silver_data_quality_check_result")

silver_pipeline_run_audit_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_pipeline_run_audit.{FILE_FORMAT}/")
)
silver_pipeline_run_audit_df.createOrReplaceTempView("silver_pipeline_run_audit")

silver_kpi_ingestion_performance_hourly_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_kpi_ingestion_performance_hourly.{FILE_FORMAT}/")
)
silver_kpi_ingestion_performance_hourly_df.createOrReplaceTempView(
    "silver_kpi_ingestion_performance_hourly"
)

# ---------------------------------------------------------------------------
# gold_project
# ---------------------------------------------------------------------------

gold_project_df = spark.sql(
    """
    SELECT
        sp.project_id AS project_id,
        sp.project_name AS project_name,
        CAST(sp.start_date AS DATE) AS start_date
    FROM silver_project sp
    """
)

gold_project_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/gold_project.csv")

# ---------------------------------------------------------------------------
# gold_sample
# ---------------------------------------------------------------------------

gold_sample_df = spark.sql(
    """
    SELECT
        ss.sample_id AS sample_id,
        ss.project_id AS project_id,
        ss.sample_external_id AS sample_external_id,
        CAST(ss.collection_ts AS TIMESTAMP) AS collection_ts,
        CAST(ss.received_ts AS TIMESTAMP) AS received_ts
    FROM silver_sample ss
    """
)

gold_sample_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/gold_sample.csv")

# ---------------------------------------------------------------------------
# gold_instrument
# ---------------------------------------------------------------------------

gold_instrument_df = spark.sql(
    """
    SELECT
        si.instrument_id AS instrument_id,
        si.machine_id AS machine_id,
        si.instrument_type AS instrument_type,
        si.manufacturer AS manufacturer,
        si.model AS model,
        CAST(si.is_active AS BOOLEAN) AS is_active
    FROM silver_instrument si
    """
)

gold_instrument_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/gold_instrument.csv")

# ---------------------------------------------------------------------------
# gold_machine_project_map
# ---------------------------------------------------------------------------

gold_machine_project_map_df = spark.sql(
    """
    SELECT
        smpm.machine_id AS machine_id,
        smpm.project_id AS project_id,
        CAST(smpm.effective_start_ts AS TIMESTAMP) AS effective_start_ts
    FROM silver_machine_project_map smpm
    """
)

gold_machine_project_map_df.coalesce(1).write.mode("overwrite").format(
    "csv"
).option("header", "true").save(f"{TARGET_PATH}/gold_machine_project_map.csv")

# ---------------------------------------------------------------------------
# gold_experiment
# ---------------------------------------------------------------------------

gold_experiment_df = spark.sql(
    """
    SELECT
        se.experiment_id AS experiment_id,
        se.project_id AS project_id,
        se.experiment_name AS experiment_name,
        se.experiment_type AS experiment_type
    FROM silver_experiment se
    """
)

gold_experiment_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/gold_experiment.csv")

# ---------------------------------------------------------------------------
# gold_run
# ---------------------------------------------------------------------------

gold_run_df = spark.sql(
    """
    SELECT
        sr.run_id AS run_id,
        sr.experiment_id AS experiment_id,
        sr.instrument_id AS instrument_id,
        sr.machine_id AS machine_id,
        CAST(sr.run_start_ts AS TIMESTAMP) AS run_start_ts,
        CAST(sr.run_end_ts AS TIMESTAMP) AS run_end_ts
    FROM silver_run sr
    """
)

gold_run_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/gold_run.csv")

# ---------------------------------------------------------------------------
# gold_experiment_observation
# ---------------------------------------------------------------------------

gold_experiment_observation_df = spark.sql(
    """
    SELECT
        seo.observation_id AS observation_id,
        seo.project_id AS project_id,
        seo.experiment_id AS experiment_id,
        seo.run_id AS run_id,
        seo.sample_id AS sample_id,
        seo.instrument_id AS instrument_id,
        seo.machine_id AS machine_id,
        seo.instrument_type AS instrument_type,
        seo.assay_type AS assay_type,
        CAST(seo.metric_value AS DOUBLE) AS metric_value,
        CAST(seo.observed_at_ts AS TIMESTAMP) AS observed_at_ts,
        CAST(seo.ingested_at_ts AS TIMESTAMP) AS ingested_at_ts,
        CAST(seo.processed_at_ts AS TIMESTAMP) AS processed_at_ts
    FROM silver_experiment_observation seo
    INNER JOIN silver_project sp
        ON seo.project_id = sp.project_id
    INNER JOIN silver_experiment se
        ON seo.experiment_id = se.experiment_id
    INNER JOIN silver_run sr
        ON seo.run_id = sr.run_id
    INNER JOIN silver_sample ss
        ON seo.sample_id = ss.sample_id
    INNER JOIN silver_instrument si
        ON seo.instrument_id = si.instrument_id
    """
)

gold_experiment_observation_df.coalesce(1).write.mode("overwrite").format(
    "csv"
).option("header", "true").save(f"{TARGET_PATH}/gold_experiment_observation.csv")

# ---------------------------------------------------------------------------
# gold_data_quality_check_result
# ---------------------------------------------------------------------------

gold_data_quality_check_result_df = spark.sql(
    """
    SELECT
        sdq.dq_result_id AS dq_result_id,
        sdq.entity_id AS entity_id,
        sdq.check_status AS check_status,
        CAST(sdq.failed_rule_count AS INT) AS failed_rule_count,
        CAST(sdq.quality_score AS DOUBLE) AS quality_score,
        CAST(sdq.checked_at_ts AS TIMESTAMP) AS checked_at_ts,
        sdq.run_id AS run_id
    FROM silver_data_quality_check_result sdq
    INNER JOIN silver_run sr
        ON sdq.run_id = sr.run_id
    """
)

gold_data_quality_check_result_df.coalesce(1).write.mode("overwrite").format(
    "csv"
).option("header", "true").save(f"{TARGET_PATH}/gold_data_quality_check_result.csv")

# ---------------------------------------------------------------------------
# gold_pipeline_run_audit
# ---------------------------------------------------------------------------

gold_pipeline_run_audit_df = spark.sql(
    """
    SELECT
        spra.pipeline_run_id AS pipeline_run_id,
        CAST(spra.run_start_ts AS TIMESTAMP) AS run_start_ts,
        CAST(spra.run_end_ts AS TIMESTAMP) AS run_end_ts
    FROM silver_pipeline_run_audit spra
    """
)

gold_pipeline_run_audit_df.coalesce(1).write.mode("overwrite").format(
    "csv"
).option("header", "true").save(f"{TARGET_PATH}/gold_pipeline_run_audit.csv")

# ---------------------------------------------------------------------------
# gold_kpi_ingestion_performance_hourly
# ---------------------------------------------------------------------------

gold_kpi_ingestion_performance_hourly_df = spark.sql(
    """
    SELECT
        CAST(skpi.kpi_hour_ts AS TIMESTAMP) AS kpi_hour_ts,
        skpi.instrument_type AS instrument_type,
        skpi.project_id AS project_id
    FROM silver_kpi_ingestion_performance_hourly skpi
    INNER JOIN silver_project sp
        ON skpi.project_id = sp.project_id
    """
)

gold_kpi_ingestion_performance_hourly_df.coalesce(1).write.mode(
    "overwrite"
).format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_kpi_ingestion_performance_hourly.csv"
)

job.commit()
