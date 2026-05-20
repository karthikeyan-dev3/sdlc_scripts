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

# -------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -------------------------------------------------------------------
stes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_trial_enrollment_statistics.{FILE_FORMAT}/")
)
stes_df.createOrReplaceTempView("silver_trial_enrollment_statistics")

sspm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_site_performance_metrics.{FILE_FORMAT}/")
)
sspm_df.createOrReplaceTempView("silver_site_performance_metrics")

srgl_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_report_generation_logs.{FILE_FORMAT}/")
)
srgl_df.createOrReplaceTempView("silver_report_generation_logs")

# -------------------------------------------------------------------
# Target: gold_clinical_trial_kpis
# -------------------------------------------------------------------
gold_clinical_trial_kpis_df = spark.sql(
    """
    SELECT
        stes.trial_id AS trial_id,
        stes.country AS country,
        stes.site_id AS site_id,
        CAST(stes.enrolling_patients_count AS INT) AS total_enrolled_patients,
        CAST(stes.enrolling_patients_count AS INT) - CAST(stes.dropout_patients_count AS INT) AS active_patients,
        CAST(stes.dropout_patients_count AS INT) AS patient_dropout_count,
        CAST(stes.visits_completed_count AS DOUBLE) / NULLIF(CAST(stes.enrolling_patients_count AS DOUBLE), 0D) AS visit_completion_percentage,
        CAST(stes.enrollment_rate AS DOUBLE) AS enrollment_trend,
        CONCAT(
            CAST(stes.enrollment_rate AS STRING), '|',
            CAST(stes.enrolling_patients_count AS STRING), '|',
            CAST(stes.dropout_patients_count AS STRING), '|',
            CAST(stes.visits_completed_count AS STRING)
        ) AS historical_data
    FROM silver_trial_enrollment_statistics stes
    """
)

(
    gold_clinical_trial_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_kpis.csv")
)

# -------------------------------------------------------------------
# Target: gold_clinical_trial_analysis
# -------------------------------------------------------------------
gold_clinical_trial_analysis_df = spark.sql(
    """
    SELECT
        sspm.trial_id AS trial_id,
        sspm.country AS country,
        sspm.site_id AS site_id,
        CASE
            WHEN CAST(sspm.underperformance_metric AS DOUBLE) < 0D THEN 'UNDERPERFORMING'
            ELSE 'ON_TRACK'
        END AS underperformance_status,
        CASE
            WHEN CAST(sspm.enrollment_delay_metric AS DOUBLE) < 0D THEN 'DELAYED'
            ELSE 'ON_TIME'
        END AS enrollment_delay_status,
        CASE
            WHEN CAST(sspm.retention_rate_metric AS DOUBLE) < 0.8D THEN 'LOW_RETENTION'
            ELSE 'GOOD_RETENTION'
        END AS patient_retention_status,
        CASE
            WHEN CAST(sspm.adherence_rate_metric AS DOUBLE) < 0.8D THEN 'LOW_ADHERENCE'
            ELSE 'GOOD_ADHERENCE'
        END AS visit_adherence_status
    FROM silver_site_performance_metrics sspm
    """
)

(
    gold_clinical_trial_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_analysis.csv")
)

# -------------------------------------------------------------------
# Target: gold_clinical_trial_reporting
# -------------------------------------------------------------------
gold_clinical_trial_reporting_df = spark.sql(
    """
    SELECT
        srgl.report_id AS report_id,
        srgl.trial_id AS trial_id,
        srgl.country AS country,
        srgl.site_id AS site_id,
        srgl.report_generated_time AS generated_timestamp,
        CAST(srgl.latency_metric AS INT) AS report_latency
    FROM silver_report_generation_logs srgl
    """
)

(
    gold_clinical_trial_reporting_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_reporting.csv")
)

job.commit()