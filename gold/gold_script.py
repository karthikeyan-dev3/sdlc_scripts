import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------
tsdk_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_daily_kpis_silver.{FILE_FORMAT}/")
)

tspf_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_performance_flags_silver.{FILE_FORMAT}/")
)

tsmt_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_metric_trends_silver.{FILE_FORMAT}/")
)

dfqs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_freshness_quality_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------
tsdk_df.createOrReplaceTempView("trial_site_daily_kpis_silver")
tspf_df.createOrReplaceTempView("trial_site_performance_flags_silver")
tsmt_df.createOrReplaceTempView("trial_site_metric_trends_silver")
dfqs_df.createOrReplaceTempView("data_freshness_quality_silver")

# ------------------------------------------------------------
# Target: gold_clinical_trial_kpis
# Latest row per (trial_id, country, site_id) by most recent kpi_date
# ------------------------------------------------------------
gold_clinical_trial_kpis_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        tsdk.trial_id AS trial_id,
        tsdk.country AS country,
        tsdk.site_id AS site_id,
        CAST(tsdk.total_enrolled_patients AS INT) AS total_enrolled_patients,
        CAST(tsdk.active_patients AS INT) AS active_patients,
        CAST(tsdk.visit_completion_percentage AS FLOAT) AS visit_completion_percentage,
        ROW_NUMBER() OVER (
          PARTITION BY tsdk.trial_id, tsdk.country, tsdk.site_id
          ORDER BY DATE(tsdk.kpi_date) DESC
        ) AS rn
      FROM trial_site_daily_kpis_silver tsdk
    )
    SELECT
      trial_id,
      country,
      site_id,
      total_enrolled_patients,
      active_patients,
      visit_completion_percentage
    FROM ranked
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

# ------------------------------------------------------------
# Target: gold_clinical_trial_performance
# Latest row per (trial_id, country, site_id) by most recent as_of_date
# ------------------------------------------------------------
gold_clinical_trial_performance_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        tspf.trial_id AS trial_id,
        tspf.country AS country,
        tspf.site_id AS site_id,
        CAST(tspf.enrollment_delay_flag AS BOOLEAN) AS enrollment_delay_flag,
        CAST(tspf.underperforming_site_flag AS BOOLEAN) AS underperforming_site_flag,
        ROW_NUMBER() OVER (
          PARTITION BY tspf.trial_id, tspf.country, tspf.site_id
          ORDER BY DATE(tspf.as_of_date) DESC
        ) AS rn
      FROM trial_site_performance_flags_silver tspf
    )
    SELECT
      trial_id,
      country,
      site_id,
      enrollment_delay_flag,
      underperforming_site_flag
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_clinical_trial_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_performance.csv")
)

# ------------------------------------------------------------
# Target: gold_clinical_trial_trends
# Map metric_date -> date
# ------------------------------------------------------------
gold_clinical_trial_trends_df = spark.sql(
    """
    SELECT
      tsmt.trial_id AS trial_id,
      tsmt.country AS country,
      tsmt.site_id AS site_id,
      tsmt.metric_name AS metric_name,
      tsmt.metric_value AS metric_value,
      DATE(tsmt.metric_date) AS date
    FROM trial_site_metric_trends_silver tsmt
    """
)

(
    gold_clinical_trial_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_trends.csv")
)

# ------------------------------------------------------------
# Target: gold_data_freshness_and_quality
# ------------------------------------------------------------
gold_data_freshness_and_quality_df = spark.sql(
    """
    SELECT
      CAST(dfqs.data_update_time AS TIMESTAMP) AS data_update_time,
      CAST(dfqs.data_quality_score AS FLOAT) AS data_quality_score,
      CAST(dfqs.reporting_latency AS INT) AS reporting_latency
    FROM data_freshness_quality_silver dfqs
    """
)

(
    gold_data_freshness_and_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_freshness_and_quality.csv")
)

job.commit()