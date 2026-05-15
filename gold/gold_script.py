import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

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
# Read source tables (S3) and create temp views
# ------------------------------------------------------------------------------------

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

das_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_silver.{FILE_FORMAT}/")
)
das_df.createOrReplaceTempView("drug_administration_silver")

aes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)
aes_df.createOrReplaceTempView("adverse_events_silver")

wds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_data_silver.{FILE_FORMAT}/")
)
wds_df.createOrReplaceTempView("wearable_data_silver")

pass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_activity_summary_silver.{FILE_FORMAT}/")
)
pass_df.createOrReplaceTempView("patient_activity_summary_silver")

# ------------------------------------------------------------------------------------
# Target: gold_patient_enrollment
# ------------------------------------------------------------------------------------

gold_patient_enrollment_df = spark.sql(
    """
    SELECT
        CAST(pes.patient_id AS STRING) AS patient_id,
        CAST(pes.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(pes.clinical_trial_id AS STRING) AS clinical_trial_id
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

# ------------------------------------------------------------------------------------
# Target: gold_clinical_visits
# ------------------------------------------------------------------------------------

gold_clinical_visits_df = spark.sql(
    """
    SELECT
        CAST(cvs.patient_id AS STRING) AS patient_id,
        CAST(cvs.visit_date AS TIMESTAMP) AS visit_date,
        CAST(cvs.visit_type AS STRING) AS visit_type,
        CAST(cvs.clinical_trial_id AS STRING) AS clinical_trial_id,
        CAST(NULL AS STRING) AS doctor_id
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

# ------------------------------------------------------------------------------------
# Target: gold_lab_results
# ------------------------------------------------------------------------------------

gold_lab_results_df = spark.sql(
    """
    SELECT
        CAST(lrs.patient_id AS STRING) AS patient_id,
        CAST(lrs.test_date AS TIMESTAMP) AS test_date,
        CAST(lrs.test_type AS STRING) AS test_type,
        CAST(lrs.result_value AS DOUBLE) AS result_value,
        CAST(lrs.result_unit AS STRING) AS result_unit
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

# ------------------------------------------------------------------------------------
# Target: gold_drug_administration
# ------------------------------------------------------------------------------------

gold_drug_administration_df = spark.sql(
    """
    SELECT
        CAST(das.patient_id AS STRING) AS patient_id,
        CAST(das.administration_date AS TIMESTAMP) AS administration_date,
        CAST(das.drug_name AS STRING) AS drug_name,
        CAST(das.dosage AS DOUBLE) AS dosage,
        CAST(das.route_of_administration AS STRING) AS route_of_administration
    FROM drug_administration_silver das
    """
)

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_adverse_events
# ------------------------------------------------------------------------------------

gold_adverse_events_df = spark.sql(
    """
    SELECT
        CAST(aes.patient_id AS STRING) AS patient_id,
        CAST(aes.event_date AS TIMESTAMP) AS event_date,
        CAST(aes.event_description AS STRING) AS event_description,
        CAST(aes.severity AS STRING) AS severity,
        CAST(aes.clinical_trial_id AS STRING) AS clinical_trial_id
    FROM adverse_events_silver aes
    """
)

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_wearable_data
# ------------------------------------------------------------------------------------

gold_wearable_data_df = spark.sql(
    """
    SELECT
        CAST(wds.patient_id AS STRING) AS patient_id,
        CAST(wds.data_timestamp AS TIMESTAMP) AS data_timestamp,
        CAST(wds.heart_rate AS FLOAT) AS heart_rate,
        CAST(wds.steps_count AS INT) AS steps_count
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

# ------------------------------------------------------------------------------------
# Target: gold_patient_summary
# ------------------------------------------------------------------------------------

gold_patient_summary_df = spark.sql(
    """
    SELECT
        CAST(pass.patient_id AS STRING) AS patient_id,
        CAST(pass.aggregate_clinical_trials AS INT) AS aggregate_clinical_trials,
        CAST(pass.total_visits AS INT) AS total_visits,
        CAST(pass.last_lab_result_date AS TIMESTAMP) AS last_lab_result_date,
        CAST(pass.adverse_event_count AS INT) AS adverse_event_count
    FROM patient_activity_summary_silver pass
    """
)

(
    gold_patient_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_summary.csv")
)

job.commit()
