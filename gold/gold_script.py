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

silver_patient_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient.{FILE_FORMAT}/")
)
silver_patient_df.createOrReplaceTempView("silver_patient")

silver_patient_visit_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_visit.{FILE_FORMAT}/")
)
silver_patient_visit_df.createOrReplaceTempView("silver_patient_visit")

silver_patient_lab_result_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_lab_result.{FILE_FORMAT}/")
)
silver_patient_lab_result_df.createOrReplaceTempView("silver_patient_lab_result")

silver_patient_drug_administration_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_drug_administration.{FILE_FORMAT}/")
)
silver_patient_drug_administration_df.createOrReplaceTempView("silver_patient_drug_administration")

silver_patient_adverse_event_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_adverse_event.{FILE_FORMAT}/")
)
silver_patient_adverse_event_df.createOrReplaceTempView("silver_patient_adverse_event")

silver_patient_wearable_observation_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_wearable_observation.{FILE_FORMAT}/")
)
silver_patient_wearable_observation_df.createOrReplaceTempView("silver_patient_wearable_observation")

silver_patient_360_daily_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_360_daily.{FILE_FORMAT}/")
)
silver_patient_360_daily_df.createOrReplaceTempView("silver_patient_360_daily")

# ------------------------------------------------------------------------------
# Target: gold.gold_patient
# Source: silver.silver_patient sp
# ------------------------------------------------------------------------------

gold_patient_df = spark.sql(
    """
    SELECT
        sp.*
    FROM silver_patient sp
    """
)

(
    gold_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_visit
# Source: silver.silver_patient_visit spv INNER JOIN silver.silver_patient sp ON spv.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_visit_df = spark.sql(
    """
    SELECT
        spv.*
    FROM silver_patient_visit spv
    INNER JOIN silver_patient sp
        ON spv.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_visit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_visit.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_lab_result
# Source: silver.silver_patient_lab_result splr INNER JOIN silver.silver_patient sp ON splr.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_lab_result_df = spark.sql(
    """
    SELECT
        splr.*
    FROM silver_patient_lab_result splr
    INNER JOIN silver_patient sp
        ON splr.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_lab_result_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_lab_result.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_drug_administration
# Source: silver.silver_patient_drug_administration spda INNER JOIN silver.silver_patient sp ON spda.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_drug_administration_df = spark.sql(
    """
    SELECT
        spda.*
    FROM silver_patient_drug_administration spda
    INNER JOIN silver_patient sp
        ON spda.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_drug_administration.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_adverse_event
# Source: silver.silver_patient_adverse_event spae INNER JOIN silver.silver_patient sp ON spae.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_adverse_event_df = spark.sql(
    """
    SELECT
        spae.*
    FROM silver_patient_adverse_event spae
    INNER JOIN silver_patient sp
        ON spae.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_adverse_event_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_adverse_event.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_wearable_observation
# Source: silver.silver_patient_wearable_observation spwo INNER JOIN silver.silver_patient sp ON spwo.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_wearable_observation_df = spark.sql(
    """
    SELECT
        spwo.*
    FROM silver_patient_wearable_observation spwo
    INNER JOIN silver_patient sp
        ON spwo.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_wearable_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_wearable_observation.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_patient_360_daily
# Source: silver.silver_patient_360_daily sp360d INNER JOIN silver.silver_patient sp ON sp360d.patient_sk = sp.patient_sk
# ------------------------------------------------------------------------------

gold_patient_360_daily_df = spark.sql(
    """
    SELECT
        sp360d.*
    FROM silver_patient_360_daily sp360d
    INNER JOIN silver_patient sp
        ON sp360d.patient_sk = sp.patient_sk
    """
)

(
    gold_patient_360_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_360_daily.csv")
)

job.commit()