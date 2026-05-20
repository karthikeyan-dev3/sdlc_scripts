import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -------------------------------------------------------------------
# Initialize Glue Job
# -------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Paths and Config
# -------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read Silver Tables
# -------------------------------------------------------------------
pds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_silver.{FILE_FORMAT}/")
)

hes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/healthcare_events_silver.{FILE_FORMAT}/")
)

ctds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_data_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create Temp Views
# -------------------------------------------------------------------
pds_df.createOrReplaceTempView("patient_data_silver")
hes_df.createOrReplaceTempView("healthcare_events_silver")
ctds_df.createOrReplaceTempView("clinical_trial_data_silver")

# -------------------------------------------------------------------
# GOLD TABLE : gold_patient_data
# -------------------------------------------------------------------
gold_patient_data_df = spark.sql(
    """
    SELECT
        CAST(pds.patient_id AS STRING) AS patient_id,
        CAST(pds.patient_name AS STRING) AS patient_name,
        CAST(pds.date_of_birth AS DATE) AS dob,
        CAST(pds.gender AS STRING) AS gender,
        CAST(pds.standardized_patient_identifier AS STRING) AS standardized_patient_identifier
    FROM patient_data_silver pds
    """
)

(
    gold_patient_data_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_data.{FILE_FORMAT}")
)

# -------------------------------------------------------------------
# GOLD TABLE : gold_healthcare_events
# -------------------------------------------------------------------
gold_healthcare_events_df = spark.sql(
    """
    SELECT
        CAST(hes.patient_id AS STRING) AS patient_id,
        CAST(hes.event_date AS TIMESTAMP) AS event_date,
        CAST(hes.event_type AS STRING) AS event_type,
        CAST(hes.event_description AS STRING) AS event_description,
        CAST(hes.healthcare_provider AS STRING) AS healthcare_provider
    FROM healthcare_events_silver hes
    """
)

(
    gold_healthcare_events_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_healthcare_events.{FILE_FORMAT}")
)

# -------------------------------------------------------------------
# GOLD TABLE : gold_clinical_trial_data
# -------------------------------------------------------------------
gold_clinical_trial_data_df = spark.sql(
    """
    SELECT
        CAST(ctds.patient_id AS STRING) AS patient_id,
        CAST(ctds.trial_id AS STRING) AS trial_id,
        CAST(ctds.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(ctds.trial_status AS STRING) AS trial_status,
        CAST(ctds.results AS STRING) AS results
    FROM clinical_trial_data_silver ctds
    """
)

(
    gold_clinical_trial_data_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_data.{FILE_FORMAT}")
)

# -------------------------------------------------------------------
# Commit Job
# -------------------------------------------------------------------
job.commit()