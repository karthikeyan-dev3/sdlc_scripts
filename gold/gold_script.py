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

# -----------------------------------------------------------------------------------
# Read Source Tables from S3
# -----------------------------------------------------------------------------------
tsr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_rollup_silver.{FILE_FORMAT}/")
)

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_performance_silver.{FILE_FORMAT}/")
)

rms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/reporting_metrics_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create Temp Views
# -----------------------------------------------------------------------------------
tsr_df.createOrReplaceTempView("trial_site_rollup_silver")
sps_df.createOrReplaceTempView("site_performance_silver")
rms_df.createOrReplaceTempView("reporting_metrics_silver")

# -----------------------------------------------------------------------------------
# Target: gold_clinical_trial_metrics
# -----------------------------------------------------------------------------------
gctm_df = spark.sql(
    """
    SELECT
        CAST(tsr.trial_id AS STRING)                         AS trial_id,
        CAST(tsr.country AS STRING)                          AS country,
        CAST(tsr.site_id AS STRING)                          AS site_id,
        CAST(tsr.total_enrolled_patients AS BIGINT)          AS total_enrolled_patients,
        CAST(tsr.dropout_count AS BIGINT)                    AS dropout_count,
        CAST(tsr.total_sites AS BIGINT)                      AS total_sites,
        CAST(tsr.metric_aggregation_time AS TIMESTAMP)       AS metric_aggregation_time
    FROM trial_site_rollup_silver tsr
    """
)

(
    gctm_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_metrics.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_clinical_performance_summary
# -----------------------------------------------------------------------------------
gcps_df = spark.sql(
    """
    SELECT
        CAST(sps.trial_id AS STRING)                   AS trial_id,
        CAST(sps.country AS STRING)                    AS country,
        CAST(sps.site_id AS STRING)                    AS site_id,
        CAST(sps.is_underperforming AS BOOLEAN)        AS is_underperforming,
        CAST(sps.performance_rating AS STRING)         AS performance_rating,
        CAST(sps.summary_time_period AS TIMESTAMP)     AS summary_time_period
    FROM site_performance_silver sps
    """
)

(
    gcps_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_performance_summary.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_reporting_metrics
# -----------------------------------------------------------------------------------
grm_df = spark.sql(
    """
    SELECT
        CAST(rms.update_timestamp AS TIMESTAMP)               AS update_timestamp,
        CAST(rms.reporting_latency_minutes AS BIGINT)         AS reporting_latency_minutes,
        CAST(rms.data_freshness_percent AS DECIMAL(5,2))      AS data_freshness_percent,
        CAST(rms.dashboard_usage_percent AS DECIMAL(5,2))     AS dashboard_usage_percent,
        CAST(rms.data_quality_score AS DECIMAL(5,2))          AS data_quality_score
    FROM reporting_metrics_silver rms
    """
)

(
    grm_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_reporting_metrics.csv")
)

job.commit()
