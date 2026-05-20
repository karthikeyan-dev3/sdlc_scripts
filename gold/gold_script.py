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

# ---------------------------
# Read Source Tables (S3)
# ---------------------------

clinical_trial_sites_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_sites_silver.{FILE_FORMAT}/")
)
clinical_trial_sites_silver_df.createOrReplaceTempView("clinical_trial_sites_silver")

performance_trends_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/performance_trends_silver.{FILE_FORMAT}/")
)
performance_trends_silver_df.createOrReplaceTempView("performance_trends_silver")

trial_enrollment_analysis_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_enrollment_analysis_silver.{FILE_FORMAT}/")
)
trial_enrollment_analysis_silver_df.createOrReplaceTempView("trial_enrollment_analysis_silver")

data_quality_checks_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_checks_silver.{FILE_FORMAT}/")
)
data_quality_checks_silver_df.createOrReplaceTempView("data_quality_checks_silver")

# ---------------------------
# Target: gold_clinical_trial_kpis
# ---------------------------

gold_clinical_trial_kpis_df = spark.sql(
    """
    SELECT
        CAST(ctss.trial_id AS STRING) AS trial_id,
        CAST(ctss.country AS STRING) AS country,
        CAST(ctss.site_id AS STRING) AS site_id,
        CAST(ctss.total_enrolled_patients AS INT) AS total_enrolled_patients,
        CAST(ctss.visit_completion_rate AS DOUBLE) AS visit_completion_percentage,
        DATE(ctss.metric_date) AS metric_date
    FROM clinical_trial_sites_silver ctss
    """
)

(
    gold_clinical_trial_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_kpis.csv")
)

# ---------------------------
# Target: gold_trial_performance_analysis
# ---------------------------

gold_trial_performance_analysis_df = spark.sql(
    """
    SELECT
        CAST(pts.trial_id AS STRING) AS trial_id,
        CAST(teas.site_id AS STRING) AS underperforming_site_id,
        CAST(teas.enrollment_delay_flag AS INT) AS enrollment_delay_flag,
        CAST(pts.trend_data AS STRING) AS trend_data,
        DATE(pts.analysis_date) AS analysis_date
    FROM performance_trends_silver pts
    INNER JOIN trial_enrollment_analysis_silver teas
        ON pts.trial_id = teas.trial_id
       AND pts.site_id = teas.site_id
       AND pts.analysis_date = teas.metric_date
    """
)

(
    gold_trial_performance_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_trial_performance_analysis.csv")
)

# ---------------------------
# Target: gold_data_quality_metrics
# ---------------------------

gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        CAST(dqcs.data_quality_score AS DOUBLE) AS data_quality_score,
        CAST(dqcs.reporting_latency AS DOUBLE) AS reporting_latency,
        CAST(dqcs.data_freshness_percentage AS DOUBLE) AS data_freshness_percentage,
        CAST(dqcs.processing_time AS DOUBLE) AS processing_time,
        DATE(dqcs.evaluation_date) AS evaluation_date
    FROM data_quality_checks_silver dqcs
    """
)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()