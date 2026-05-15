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

# -----------------------------
# Read source tables from S3
# -----------------------------
pes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollments_silver.{FILE_FORMAT}/")
)
cvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visits_silver.{FILE_FORMAT}/")
)
lrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_silver.{FILE_FORMAT}/")
)
das_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administrations_silver.{FILE_FORMAT}/")
)
aes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)
wms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_metrics_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
pes_df.createOrReplaceTempView("patient_enrollments_silver")
cvs_df.createOrReplaceTempView("clinical_visits_silver")
lrs_df.createOrReplaceTempView("lab_results_silver")
das_df.createOrReplaceTempView("drug_administrations_silver")
aes_df.createOrReplaceTempView("adverse_events_silver")
wms_df.createOrReplaceTempView("wearable_metrics_silver")

# =========================================================
# Target: gold_patient_enrollment
# =========================================================
gold_patient_enrollment_df = spark.sql(
    """
    SELECT
        CAST(pes.patient_id AS STRING) AS patient_id,
        CAST(pes.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(pes.source_system AS STRING) AS source_system,
        CAST(pes.study_id AS STRING) AS study_id
    FROM patient_enrollments_silver pes
    """
)

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# =========================================================
# Target: gold_clinical_visits
# =========================================================
gold_clinical_visits_df = spark.sql(
    """
    SELECT
        CAST(cvs.patient_id AS STRING) AS patient_id,
        CAST(cvs.visit_date AS TIMESTAMP) AS visit_date,
        CAST(cvs.visit_type AS STRING) AS visit_type,
        CAST(cvs.practitioner_id AS STRING) AS practitioner_id,
        CAST(cvs.notes AS STRING) AS notes
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

# =========================================================
# Target: gold_lab_results
# =========================================================
gold_lab_results_df = spark.sql(
    """
    SELECT
        CAST(lrs.patient_id AS STRING) AS patient_id,
        CAST(lrs.test_date AS TIMESTAMP) AS test_date,
        CAST(lrs.test_type AS STRING) AS test_type,
        CAST(lrs.test_result AS DOUBLE) AS test_result,
        CAST(lrs.units AS STRING) AS units,
        CAST(lrs.reference_range AS STRING) AS reference_range
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

# =========================================================
# Target: gold_drug_administration
# =========================================================
gold_drug_administration_df = spark.sql(
    """
    SELECT
        CAST(das.patient_id AS STRING) AS patient_id,
        CAST(das.administration_date AS TIMESTAMP) AS administration_date,
        CAST(das.drug_name AS STRING) AS drug_name,
        CAST(das.dosage AS DOUBLE) AS dosage,
        CAST(das.route AS STRING) AS route,
        CAST(das.frequency AS STRING) AS frequency
    FROM drug_administrations_silver das
    """
)

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# =========================================================
# Target: gold_adverse_events
# =========================================================
gold_adverse_events_df = spark.sql(
    """
    SELECT
        CAST(aes.patient_id AS STRING) AS patient_id,
        CAST(aes.event_date AS TIMESTAMP) AS event_date,
        CAST(aes.event_type AS STRING) AS event_type,
        CAST(aes.severity AS STRING) AS severity,
        CAST(aes.outcome AS STRING) AS outcome,
        CAST(aes.notes AS STRING) AS notes
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

# =========================================================
# Target: gold_wearable_data
# =========================================================
gold_wearable_data_df = spark.sql(
    """
    SELECT
        CAST(wms.patient_id AS STRING) AS patient_id,
        CAST(wms.collection_date AS TIMESTAMP) AS collection_date,
        CAST(wms.device_type AS STRING) AS device_type,
        CAST(wms.heart_rate AS FLOAT) AS heart_rate,
        CAST(wms.steps AS INT) AS steps,
        CAST(wms.activity_duration AS STRING) AS activity_duration
    FROM wearable_metrics_silver wms
    """
)

(
    gold_wearable_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_data.csv")
)

# =========================================================
# Target: gold_patient_analytics
# Notes:
# - UDT provides mapping for patient_id only.
# - Other derived fields described (start_date, end_date, scores, compliance_status)
#   do not have exact UDT column mappings/rules, so they are not generated here.
# =========================================================
gold_patient_analytics_df = spark.sql(
    """
    SELECT
        CAST(pes.patient_id AS STRING) AS patient_id
    FROM patient_enrollments_silver pes
    LEFT JOIN clinical_visits_silver cvs
        ON pes.patient_id = cvs.patient_id
    LEFT JOIN lab_results_silver lrs
        ON pes.patient_id = lrs.patient_id
    LEFT JOIN drug_administrations_silver das
        ON pes.patient_id = das.patient_id
    LEFT JOIN adverse_events_silver aes
        ON pes.patient_id = aes.patient_id
    LEFT JOIN wearable_metrics_silver wms
        ON pes.patient_id = wms.patient_id
    """
)

(
    gold_patient_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_analytics.csv")
)

job.commit()
