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

# ------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------------------------

ctscs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_site_current_silver.{FILE_FORMAT}/")
)
ctscs_df.createOrReplaceTempView("clinical_trial_site_current_silver")

ctsds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_site_day_silver.{FILE_FORMAT}/")
)
ctsds_df.createOrReplaceTempView("clinical_trial_site_day_silver")

# ------------------------------------------------------------------------------
# Target: gold.clinical_trial_metrics
# ------------------------------------------------------------------------------

clinical_trial_metrics_df = spark.sql(
    """
    SELECT
        CAST(ctscs.trial_id AS STRING)                      AS trial_id,
        CAST(ctscs.country AS STRING)                       AS country,
        CAST(ctscs.site_id AS STRING)                       AS site_id,
        CAST(ctscs.total_enrolled_patients AS BIGINT)       AS total_enrolled_patients,
        CAST(ctscs.active_patients AS BIGINT)               AS active_patients,
        CAST(ctscs.patient_dropout_count AS BIGINT)         AS patient_dropout_count,
        CAST(ctscs.visit_completion_percentage AS DOUBLE)   AS visit_completion_percentage,
        CAST(ctscs.enrollment_trend AS DOUBLE)              AS enrollment_trend,
        CAST(ctscs.underperforming_sites_flag AS BOOLEAN)   AS underperforming_sites_flag,
        CAST(ctscs.enrollment_delay_flag AS BOOLEAN)        AS enrollment_delay_flag,
        CAST(ctscs.patient_retention_rate AS DOUBLE)        AS patient_retention_rate,
        CAST(ctscs.visit_adherence_rate AS DOUBLE)          AS visit_adherence_rate,
        CAST(ctscs.data_refresh_time AS TIMESTAMP)          AS data_refresh_time
    FROM clinical_trial_site_current_silver ctscs
    """
)

(
    clinical_trial_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_metrics.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.clinical_trial_performance_trends
# ------------------------------------------------------------------------------

clinical_trial_performance_trends_df = spark.sql(
    """
    SELECT
        CAST(ctsds.trial_id AS STRING)                       AS trial_id,
        CAST(ctsds.trend_date AS DATE)                       AS trend_date,
        CAST(ctsds.enrollment_trend_value AS DOUBLE)         AS enrollment_trend,
        CAST(ctsds.dropout_trend_value AS DOUBLE)            AS dropout_trend,
        CAST(ctsds.visit_completion_trend_value AS DOUBLE)   AS visit_completion_trend
    FROM clinical_trial_site_day_silver ctsds
    """
)

(
    clinical_trial_performance_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_performance_trends.csv")
)

job.commit()