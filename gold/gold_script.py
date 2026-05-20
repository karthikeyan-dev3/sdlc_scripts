import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Source Reads (Silver)
# -------------------------------------------------------------------
tsdm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_daily_metrics_silver.{FILE_FORMAT}/")
)
tsdm_df.createOrReplaceTempView("trial_site_daily_metrics_silver")

sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_alerts_silver.{FILE_FORMAT}/")
)
sas_df.createOrReplaceTempView("site_alerts_silver")

# -------------------------------------------------------------------
# Target: gold.gold_clinical_trial_metrics
# Mappings available in UDT:
#   gctm.trial_id = tsdm.trial_id
#   gctm.country  = tsdm.country
#   gctm.site_id  = tsdm.site_id
# -------------------------------------------------------------------
gold_clinical_trial_metrics_df = spark.sql(
    """
    SELECT
        CAST(tsdm.trial_id AS STRING)  AS trial_id,
        CAST(tsdm.country  AS STRING)  AS country,
        CAST(tsdm.site_id  AS STRING)  AS site_id
    FROM trial_site_daily_metrics_silver tsdm
    """
)

(
    gold_clinical_trial_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_metrics.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_clinical_performance_reporting
# Mappings available in UDT:
#   gcpr.reporting_date = tsdm.metric_date
# -------------------------------------------------------------------
gold_clinical_performance_reporting_df = spark.sql(
    """
    SELECT
        CAST(tsdm.metric_date AS DATE) AS reporting_date
    FROM trial_site_daily_metrics_silver tsdm
    """
)

(
    gold_clinical_performance_reporting_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_performance_reporting.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_stakeholder_insights
# No column-level transformations provided in UDT for this target.
# Producing an empty schema-less CSV is not valid; therefore, select no rows with no columns is avoided.
# Creating a minimal dataset is not allowed (would invent columns). Hence, write an empty file with zero columns is skipped.
# -------------------------------------------------------------------

job.commit()