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

# =============================================================================
# Read Source Tables
# =============================================================================
ctkds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_kpi_daily_silver.{FILE_FORMAT}/")
)
ctkds_df.createOrReplaceTempView("clinical_trial_kpi_daily_silver")

hkrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/historical_kpi_records_silver.{FILE_FORMAT}/")
)
hkrs_df.createOrReplaceTempView("historical_kpi_records_silver")

spmds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_performance_metrics_daily_silver.{FILE_FORMAT}/")
)
spmds_df.createOrReplaceTempView("site_performance_metrics_daily_silver")

# =============================================================================
# Target: gold_clinical_trial_kpis
# =============================================================================
gold_clinical_trial_kpis_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(ctkds.trial_id AS STRING) AS trial_id,
        CAST(ctkds.country AS STRING) AS country,
        CAST(ctkds.site_id AS STRING) AS site_id,
        CAST(ctkds.total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
        CAST(ctkds.active_patients AS BIGINT) AS active_patients,
        CAST(ctkds.patient_dropout_count AS BIGINT) AS patient_dropout_count,
        CAST(ctkds.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
        DATE(ctkds.date) AS date,
        LAG(CAST(ctkds.total_enrolled_patients AS BIGINT)) OVER (
          PARTITION BY ctkds.trial_id, ctkds.country, ctkds.site_id
          ORDER BY DATE(ctkds.date)
        ) AS prev_total_enrolled_patients,
        ROW_NUMBER() OVER (
          PARTITION BY ctkds.trial_id, ctkds.country, ctkds.site_id
          ORDER BY DATE(ctkds.date) DESC
        ) AS rn
      FROM clinical_trial_kpi_daily_silver ctkds
    )
    SELECT
      trial_id,
      country,
      site_id,
      total_enrolled_patients,
      active_patients,
      patient_dropout_count,
      visit_completion_percentage,
      CASE
        WHEN total_enrolled_patients > prev_total_enrolled_patients THEN 'INCREASING'
        WHEN total_enrolled_patients < prev_total_enrolled_patients THEN 'DECREASING'
        ELSE 'FLAT'
      END AS enrollment_trend
    FROM base
    WHERE rn = 1
    """
)

(
    gold_clinical_trial_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_kpis.csv")
)

# =============================================================================
# Target: gold_historical_kpi_trends
# =============================================================================
gold_historical_kpi_trends_df = spark.sql(
    """
    SELECT
      CAST(hkrs.trial_id AS STRING) AS trial_id,
      DATE(hkrs.date) AS date,
      CAST(hkrs.total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
      CAST(hkrs.patient_dropout_count AS BIGINT) AS patient_dropout_count,
      CAST(hkrs.visit_completion_percentage AS DOUBLE) AS visit_completion_percentage,
      CAST(hkrs.data_quality_score AS DOUBLE) AS data_quality_score
    FROM historical_kpi_records_silver hkrs
    """
)

(
    gold_historical_kpi_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_historical_kpi_trends.csv")
)

# =============================================================================
# Target: gold_site_performance
# =============================================================================
gold_site_performance_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(spmds.site_id AS STRING) AS site_id,
        CAST(spmds.patient_retention_rate AS DOUBLE) AS patient_retention_rate,
        CAST(spmds.visit_adherence_rate AS DOUBLE) AS visit_adherence_rate,
        DATE(spmds.date) AS date,
        SUM(
          CASE
            WHEN (spmds.patient_retention_rate IS NOT NULL OR spmds.visit_adherence_rate IS NOT NULL) THEN 1
            ELSE 0
          END
        ) OVER (
          PARTITION BY spmds.site_id
          ORDER BY DATE(spmds.date)
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS recent_non_null_cnt,
        ROW_NUMBER() OVER (
          PARTITION BY spmds.site_id
          ORDER BY DATE(spmds.date) DESC
        ) AS rn
      FROM site_performance_metrics_daily_silver spmds
    )
    SELECT
      site_id,
      patient_retention_rate,
      visit_adherence_rate,
      CASE
        WHEN patient_retention_rate < 0.8 OR visit_adherence_rate < 80 THEN 'Y'
        ELSE 'N'
      END AS underperformance_flag,
      CASE
        WHEN recent_non_null_cnt = 0 THEN 'Y'
        ELSE 'N'
      END AS enrollment_delay_detected
    FROM base
    WHERE rn = 1
    """
)

(
    gold_site_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_site_performance.csv")
)

job.commit()
