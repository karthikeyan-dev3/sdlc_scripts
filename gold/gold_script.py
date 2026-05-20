import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read source tables from S3 and create temp views
# -------------------------------------------------------------------

pes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_silver.{FILE_FORMAT}/")
)
pes_df.createOrReplaceTempView("patient_enrollment_silver")

cvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visits_silver.{FILE_FORMAT}/")
)
cvs_df.createOrReplaceTempView("clinical_visits_silver")

lrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_silver.{FILE_FORMAT}/")
)
lrs_df.createOrReplaceTempView("lab_results_silver")

wds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_data_silver.{FILE_FORMAT}/")
)
wds_df.createOrReplaceTempView("wearable_data_silver")

das_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_audit_silver.{FILE_FORMAT}/")
)
das_df.createOrReplaceTempView("data_audit_silver")

uacs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/user_access_control_silver.{FILE_FORMAT}/")
)
uacs_df.createOrReplaceTempView("user_access_control_silver")

# -------------------------------------------------------------------
# Target: gold_patient_enrollment
# -------------------------------------------------------------------

gold_patient_enrollment_df = spark.sql(
    """
SELECT
  CAST(pes.patient_id AS STRING)     AS patient_id,
  CAST(pes.enrollment_date AS DATE)  AS enrollment_date,
  CAST(pes.trial_id AS STRING)       AS trial_id,
  CAST(pes.site_id AS STRING)        AS site_id
FROM patient_enrollment_silver pes
"""
)

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# -------------------------------------------------------------------
# Target: gold_clinical_visits
# -------------------------------------------------------------------

gold_clinical_visits_df = spark.sql(
    """
SELECT
  CAST(cvs.patient_id AS STRING)        AS patient_id,
  CAST(cvs.visit_date AS DATE)          AS visit_date,
  CAST(cvs.visit_type AS STRING)        AS visit_type,
  CAST(cvs.trial_id AS STRING)          AS trial_id,
  CAST(cvs.adherence_status AS STRING)  AS adherence_status
FROM clinical_visits_silver cvs
"""
)

(
    gold_clinical_visits_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_visits.csv")
)

# -------------------------------------------------------------------
# Target: gold_lab_results
# -------------------------------------------------------------------

gold_lab_results_df = spark.sql(
    """
SELECT
  CAST(lrs.patient_id AS STRING)    AS patient_id,
  CAST(lrs.result_date AS DATE)     AS result_date,
  CAST(lrs.test_type AS STRING)     AS test_type,
  CAST(lrs.test_result AS DOUBLE)   AS test_result,
  CAST(lrs.trial_id AS STRING)      AS trial_id
FROM lab_results_silver lrs
"""
)

(
    gold_lab_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_results.csv")
)

# -------------------------------------------------------------------
# Target: gold_wearable_data
# -------------------------------------------------------------------

gold_wearable_data_df = spark.sql(
    """
SELECT
  CAST(wds.patient_id AS STRING)           AS patient_id,
  CAST(wds.device_id AS STRING)            AS device_id,
  CAST(wds.measurement_date AS DATE)       AS measurement_date,
  CAST(wds.measurement_type AS STRING)     AS measurement_type,
  CAST(wds.measurement_value AS DOUBLE)    AS measurement_value,
  CAST(wds.trial_id AS STRING)             AS trial_id
FROM wearable_data_silver wds
"""
)

(
    gold_wearable_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_data.csv")
)

# -------------------------------------------------------------------
# Target: gold_data_audit
# -------------------------------------------------------------------

gold_data_audit_df = spark.sql(
    """
SELECT
  CAST(das.record_id AS STRING)        AS record_id,
  CAST(das.table_name AS STRING)       AS table_name,
  CAST(das.operation_type AS STRING)   AS operation_type,
  CAST(das.timestamp AS TIMESTAMP)     AS timestamp,
  CAST(das.user_id AS STRING)          AS user_id
FROM data_audit_silver das
"""
)

(
    gold_data_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_audit.csv")
)

# -------------------------------------------------------------------
# Target: gold_user_access_control
# -------------------------------------------------------------------

gold_user_access_control_df = spark.sql(
    """
SELECT
  CAST(uacs.user_id AS STRING)          AS user_id,
  CAST(uacs.role_id AS STRING)          AS role_id,
  CAST(uacs.access_level AS STRING)     AS access_level,
  CAST(uacs.assignment_date AS DATE)    AS assignment_date
FROM user_access_control_silver uacs
"""
)

(
    gold_user_access_control_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_user_access_control.csv")
)

job.commit()
