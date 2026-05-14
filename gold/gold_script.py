import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# Read source tables from S3 (Silver)
# -------------------------------------------------------------------
aers_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_reports_silver.{FILE_FORMAT}/")
)
aers_df.createOrReplaceTempView("adverse_event_reports_silver")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("patient_master_silver")

dms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_master_silver.{FILE_FORMAT}/")
)
dms_df.createOrReplaceTempView("drug_master_silver")

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_performance_silver.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("site_performance_silver")

atds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_trial_data_silver.{FILE_FORMAT}/")
)
atds_df.createOrReplaceTempView("aggregated_trial_data_silver")

# -------------------------------------------------------------------
# Target: gold.gold_adverse_event_reports
# Source: silver.adverse_event_reports_silver aers
# -------------------------------------------------------------------
gold_adverse_event_reports_df = spark.sql(
    """
    SELECT
        CAST(aers.adverse_event_id AS STRING) AS adverse_event_id,
        CAST(aers.patient_id AS STRING) AS patient_id,
        CAST(aers.drug_id AS STRING) AS drug_id,
        CAST(aers.site_id AS STRING) AS site_id,
        CAST(aers.event_date AS DATE) AS event_date,
        CAST(aers.severity AS STRING) AS severity,
        CAST(aers.description AS STRING) AS description
    FROM adverse_event_reports_silver aers
    """
)

(
    gold_adverse_event_reports_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_event_reports.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_patient_master
# Source: silver.patient_master_silver pms
# -------------------------------------------------------------------
gold_patient_master_df = spark.sql(
    """
    SELECT
        CAST(pms.patient_id AS STRING) AS patient_id,
        CAST(pms.age AS DOUBLE) AS age,
        CAST(pms.gender AS STRING) AS gender
    FROM patient_master_silver pms
    """
)

(
    gold_patient_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_master.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_drug_master
# Source: silver.drug_master_silver dms
# -------------------------------------------------------------------
gold_drug_master_df = spark.sql(
    """
    SELECT
        CAST(dms.drug_id AS STRING) AS drug_id,
        CAST(dms.drug_name AS STRING) AS drug_name
    FROM drug_master_silver dms
    """
)

(
    gold_drug_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_master.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_site_performance
# Source: silver.site_performance_silver sps
# -------------------------------------------------------------------
gold_site_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.site_id AS STRING) AS site_id,
        CAST(sps.location AS STRING) AS location
    FROM site_performance_silver sps
    """
)

(
    gold_site_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_site_performance.csv")
)

# -------------------------------------------------------------------
# Target: gold.gold_aggregated_trial_data
# Source: silver.aggregated_trial_data_silver atds
# -------------------------------------------------------------------
gold_aggregated_trial_data_df = spark.sql(
    """
    SELECT
        CAST(atds.trial_id AS STRING) AS trial_id,
        CAST(atds.total_adverse_events AS BIGINT) AS total_adverse_events,
        CAST(atds.average_severity AS DOUBLE) AS average_severity,
        CAST(atds.unique_patients AS BIGINT) AS unique_patients,
        CAST(atds.drugs_involved AS BIGINT) AS drugs_involved,
        CAST(atds.sites_involved AS BIGINT) AS sites_involved
    FROM aggregated_trial_data_silver atds
    """
)

(
    gold_aggregated_trial_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_trial_data.csv")
)

job.commit()