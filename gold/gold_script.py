import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Read Source Tables (S3)
# ------------------------------------------------------------------------------------
site_trial_kpi_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_trial_kpi_daily_silver.{FILE_FORMAT}/")
)

clinical_trial_site_dim_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_site_dim_silver.{FILE_FORMAT}/")
)

site_trial_performance_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_trial_performance_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------------------------
site_trial_kpi_daily_silver_df.createOrReplaceTempView("site_trial_kpi_daily_silver")
clinical_trial_site_dim_silver_df.createOrReplaceTempView("clinical_trial_site_dim_silver")
site_trial_performance_silver_df.createOrReplaceTempView("site_trial_performance_silver")

# ------------------------------------------------------------------------------------
# Target: gold_clinical_trial_metrics
# mapping_details: silver.site_trial_kpi_daily_silver stk
#   INNER JOIN silver.clinical_trial_site_dim_silver ctsd
#   ON stk.trial_id = ctsd.trial_id AND stk.country = ctsd.country AND stk.site_id = ctsd.site_id
# ------------------------------------------------------------------------------------
gold_clinical_trial_metrics_df = spark.sql(
    """
    SELECT
        CAST(stk.trial_id AS STRING) AS trial_id,
        CAST(ctsd.country AS STRING) AS country,
        CAST(stk.site_id AS STRING) AS site_id,
        CAST(stk.total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
        CAST(stk.active_patients AS BIGINT) AS active_patients,
        CAST(stk.patient_dropout_count AS BIGINT) AS patient_dropout_count,
        CAST(stk.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
        DATE(stk.kpi_date) AS enrollment_trends
    FROM site_trial_kpi_daily_silver stk
    INNER JOIN clinical_trial_site_dim_silver ctsd
        ON stk.trial_id = ctsd.trial_id
       AND stk.country = ctsd.country
       AND stk.site_id = ctsd.site_id
    """
)

gold_clinical_trial_metrics_output_path = f"{TARGET_PATH}/gold_clinical_trial_metrics.csv"
(
    gold_clinical_trial_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_clinical_trial_metrics_output_path)
)

# ------------------------------------------------------------------------------------
# Target: gold_clinical_trial_performance
# mapping_details: silver.site_trial_performance_silver stp
#   INNER JOIN silver.clinical_trial_site_dim_silver ctsd
#   ON stp.trial_id = ctsd.trial_id AND stp.country = ctsd.country AND stp.site_id = ctsd.site_id
# ------------------------------------------------------------------------------------
gold_clinical_trial_performance_df = spark.sql(
    """
    SELECT
        CAST(stp.trial_id AS STRING) AS trial_id,
        CAST(ctsd.country AS STRING) AS country,
        CAST(stp.site_id AS STRING) AS site_id,
        CAST(stp.underperforming_flag AS BOOLEAN) AS underperforming_flag,
        CAST(stp.enrollment_delay_flag AS BOOLEAN) AS enrollment_delay_flag,
        CAST(stp.patient_retention_rate AS DOUBLE) AS patient_retention_rate,
        CAST(stp.visit_adherence_rate AS DOUBLE) AS visit_adherence_rate,
        CAST(stp.historical_reports AS STRING) AS historical_reports
    FROM site_trial_performance_silver stp
    INNER JOIN clinical_trial_site_dim_silver ctsd
        ON stp.trial_id = ctsd.trial_id
       AND stp.country = ctsd.country
       AND stp.site_id = ctsd.site_id
    """
)

gold_clinical_trial_performance_output_path = f"{TARGET_PATH}/gold_clinical_trial_performance.csv"
(
    gold_clinical_trial_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_clinical_trial_performance_output_path)
)

job.commit()