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

# ------------------------------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------------------------------
tscks_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_current_kpis_silver.{FILE_FORMAT}/")
)
tsdks_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_daily_kpis_silver.{FILE_FORMAT}/")
)
tsams_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_aggregated_metrics_silver.{FILE_FORMAT}/")
)
dms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dashboard_metrics_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------------------------------
tscks_df.createOrReplaceTempView("trial_site_current_kpis_silver")
tsdks_df.createOrReplaceTempView("trial_site_daily_kpis_silver")
tsams_df.createOrReplaceTempView("trial_site_aggregated_metrics_silver")
dms_df.createOrReplaceTempView("dashboard_metrics_silver")

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_clinical_trial_kpis
# SOURCE: silver.trial_site_current_kpis_silver tscks
# ------------------------------------------------------------------------------------
gold_clinical_trial_kpis_df = spark.sql(
    """
    SELECT
        CAST(tscks.trial_id AS STRING) AS trial_id,
        CAST(tscks.country AS STRING) AS country,
        CAST(tscks.site_id AS STRING) AS site_id,
        CAST(tscks.total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
        CAST(tscks.active_patients AS BIGINT) AS active_patients,
        CAST(tscks.patient_dropout_count AS BIGINT) AS patient_dropout_count,
        CAST(tscks.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
        CAST(tscks.enrollment_trend AS BIGINT) AS enrollment_trend,
        CAST(tscks.data_capture_timestamp AS TIMESTAMP) AS data_capture_timestamp
    FROM trial_site_current_kpis_silver tscks
    """
)

(
    gold_clinical_trial_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_kpis.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_historical_performance
# SOURCE: silver.trial_site_daily_kpis_silver tsdks
# ------------------------------------------------------------------------------------
gold_historical_performance_df = spark.sql(
    """
    SELECT
        CAST(tsdks.trial_id AS STRING) AS trial_id,
        CAST(tsdks.country AS STRING) AS country,
        CAST(tsdks.site_id AS STRING) AS site_id,
        DATE(CAST(tsdks.date AS DATE)) AS date,
        CAST(tsdks.total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
        CAST(tsdks.active_patients AS BIGINT) AS active_patients,
        CAST(tsdks.patient_dropout_count AS BIGINT) AS patient_dropout_count,
        CAST(tsdks.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
        CAST(tsdks.enrollment_trend AS BIGINT) AS enrollment_trend
    FROM trial_site_daily_kpis_silver tsdks
    """
)

(
    gold_historical_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_historical_performance.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_aggregated_metrics
# SOURCE: silver.trial_site_aggregated_metrics_silver tsams
# ------------------------------------------------------------------------------------
gold_aggregated_metrics_df = spark.sql(
    """
    SELECT
        CAST(tsams.trial_id AS STRING) AS trial_id,
        CAST(tsams.country AS STRING) AS country,
        CAST(tsams.site_id AS STRING) AS site_id,
        CAST(tsams.aggregated_enrolment_count AS BIGINT) AS aggregated_enrolment_count,
        CAST(tsams.aggregated_dropout_count AS BIGINT) AS aggregated_dropout_count,
        CAST(tsams.aggregated_completion_rate AS DOUBLE) AS aggregated_completion_rate,
        CAST(tsams.aggregated_enrollment_trend AS DOUBLE) AS aggregated_enrollment_trend
    FROM trial_site_aggregated_metrics_silver tsams
    """
)

(
    gold_aggregated_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_metrics.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_dashboard_metrics
# SOURCE: silver.dashboard_metrics_silver dms
# ------------------------------------------------------------------------------------
gold_dashboard_metrics_df = spark.sql(
    """
    SELECT
        CAST(dms.metric_id AS STRING) AS metric_id,
        CAST(dms.metric_name AS STRING) AS metric_name,
        CAST(dms.value AS DOUBLE) AS value,
        CAST(dms.last_updated AS TIMESTAMP) AS last_updated
    FROM dashboard_metrics_silver dms
    """
)

(
    gold_dashboard_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dashboard_metrics.csv")
)

job.commit()