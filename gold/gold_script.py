import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =========================
# Read source tables from S3
# =========================
tes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_enrollment_silver.{FILE_FORMAT}/")
)
tps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_progress_silver.{FILE_FORMAT}/")
)
cts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trials_silver.{FILE_FORMAT}/")
)
kms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/kpi_metrics_silver.{FILE_FORMAT}/")
)

# =========================
# Create temp views
# =========================
tes_df.createOrReplaceTempView("trial_enrollment_silver")
tps_df.createOrReplaceTempView("trial_progress_silver")
cts_df.createOrReplaceTempView("clinical_trials_silver")
kms_df.createOrReplaceTempView("kpi_metrics_silver")

# ============================================================
# Target: gold.clinical_trial_dashboard (gctd)
# ============================================================
clinical_trial_dashboard_df = spark.sql(
    """
    SELECT
        CAST(tes.trial_id AS STRING) AS trial_id,
        CAST(tes.country AS STRING) AS country,
        CAST(tes.site_id AS STRING) AS site_id,
        CAST(tes.enrolled_patients_count AS BIGINT) AS total_enrolled_patients,
        CAST(tes.currently_active_patients AS BIGINT) AS currently_active_patients,
        CAST(tes.patient_dropout_count AS BIGINT) AS patient_dropout_count,
        CAST(tps.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
        CAST(tps.enrollment_trend AS BIGINT) AS enrollment_trend,
        greatest(
            CAST(tes.data_timestamp AS TIMESTAMP),
            CAST(tps.data_timestamp AS TIMESTAMP)
        ) AS data_timestamp
    FROM trial_enrollment_silver tes
    LEFT JOIN trial_progress_silver tps
        ON tes.trial_id = tps.trial_id
       AND tes.site_id = tps.site_id
       AND tes.country = tps.country
       AND tes.snapshot_date = tps.snapshot_date
    LEFT JOIN clinical_trials_silver cts
        ON tes.trial_id = cts.trial_id
       AND tes.site_id = cts.site_id
       AND tes.country = cts.country
    """
)

(
    clinical_trial_dashboard_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_dashboard.csv")
)

# ============================================================
# Target: gold.clinical_kpi_metrics (gckm)
# ============================================================
clinical_kpi_metrics_df = spark.sql(
    """
    SELECT
        CAST(kms.metric_id AS STRING) AS metric_id,
        CAST(kms.trial_id AS STRING) AS trial_id,
        CAST(kms.reporting_latency AS BIGINT) AS reporting_latency,
        CAST(kms.data_freshness AS BIGINT) AS data_freshness,
        CAST(kms.dashboard_adoption_rate AS DOUBLE) AS dashboard_adoption_rate,
        CAST(kms.data_quality_score AS DOUBLE) AS data_quality_score,
        CAST(kms.processing_efficiency AS BIGINT) AS processing_efficiency,
        CAST(kms.evaluation_timestamp AS TIMESTAMP) AS evaluation_timestamp
    FROM kpi_metrics_silver kms
    """
)

(
    clinical_kpi_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_kpi_metrics.csv")
)

job.commit()