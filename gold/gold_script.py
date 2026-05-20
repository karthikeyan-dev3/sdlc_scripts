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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
tscdm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_country_daily_metrics_silver.{FILE_FORMAT}/")
)

tdils_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_data_ingestion_latency_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
tscdm_df.createOrReplaceTempView("trial_site_country_daily_metrics_silver")
tdils_df.createOrReplaceTempView("trial_data_ingestion_latency_silver")

# -------------------------------------------------------------------
# 3) Transformations using Spark SQL
# -------------------------------------------------------------------

# Target: gold.gold_clinical_trial_summary
gold_clinical_trial_summary_df = spark.sql("""
SELECT
  CAST(tscdm.trial_id AS STRING) AS trial_id,
  CAST(tscdm.country AS STRING) AS country,
  CAST(tscdm.site_id AS STRING) AS site_id,
  SUM(CAST(tscdm.total_enrolled_patients AS BIGINT)) AS total_enrolled_patients,
  SUM(CAST(tscdm.active_patients AS BIGINT)) AS active_patients,
  SUM(CAST(tscdm.patient_dropout_count AS BIGINT)) AS patient_dropout_count,
  AVG(CAST(tscdm.visit_completion_percentage AS DOUBLE)) AS visit_completion_percentage,
  SUM(CAST(tscdm.enrollment_trend AS BIGINT)) AS enrollment_trend
FROM trial_site_country_daily_metrics_silver tscdm
GROUP BY
  CAST(tscdm.trial_id AS STRING),
  CAST(tscdm.country AS STRING),
  CAST(tscdm.site_id AS STRING)
""")

# Target: gold.gold_clinical_trial_real_time_data
gold_clinical_trial_real_time_data_df = spark.sql("""
SELECT
  CAST(tdils.trial_id AS STRING) AS trial_id,
  MAX(CAST(tdils.data_ingestion_timestamp AS TIMESTAMP)) AS data_ingestion_timestamp,
  MAX(tdils.metric_update_latency) AS metric_update_latency
FROM trial_data_ingestion_latency_silver tdils
GROUP BY
  CAST(tdils.trial_id AS STRING)
""")

# Target: gold.gold_clinical_trial_historical_trends
gold_clinical_trial_historical_trends_df = spark.sql("""
SELECT
  CAST(tscdm.trial_id AS STRING) AS trial_id,
  CAST(tscdm.country AS STRING) AS country,
  CAST(tscdm.site_id AS STRING) AS site_id,
  DATE(CAST(tscdm.date AS DATE)) AS date,
  CAST(tscdm.enrollment_trend AS BIGINT) AS enrollment_trend,
  CAST(tscdm.retention_trend AS BIGINT) AS retention_trend
FROM trial_site_country_daily_metrics_silver tscdm
""")

# -------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# -------------------------------------------------------------------
(
    gold_clinical_trial_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_summary.csv")
)

(
    gold_clinical_trial_real_time_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_real_time_data.csv")
)

(
    gold_clinical_trial_historical_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_historical_trends.csv")
)

job.commit()