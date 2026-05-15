import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ======================================================================================
# Read Source Tables (S3 -> DataFrames)
# ======================================================================================

patient_enrollment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
patient_enrollment_bronze_df.createOrReplaceTempView("patient_enrollment_bronze")

clinical_visit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
)
clinical_visit_bronze_df.createOrReplaceTempView("clinical_visit_bronze")

lab_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
lab_results_bronze_df.createOrReplaceTempView("lab_results_bronze")

drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
drug_administration_bronze_df.createOrReplaceTempView("drug_administration_bronze")

adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
adverse_events_bronze_df.createOrReplaceTempView("adverse_events_bronze")

wearable_monitoring_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)
wearable_monitoring_bronze_df.createOrReplaceTempView("wearable_monitoring_bronze")

# ======================================================================================
# Target: patient_enrollments_silver
# ======================================================================================

patient_enrollments_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(peb.patient_id) AS patient_id,
            CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
            TRIM(peb.source_system) AS source_system,
            TRIM(peb.trial_id) AS study_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(peb.patient_id),
                    TRIM(peb.trial_id),
                    CAST(peb.enrollment_date AS TIMESTAMP),
                    TRIM(peb.source_system)
                ORDER BY
                    TRIM(peb.patient_id)
            ) AS rn
        FROM patient_enrollment_bronze peb
        WHERE COALESCE(TRIM(peb.patient_id), '') <> ''
    )
    SELECT
        patient_id,
        enrollment_date,
        source_system,
        study_id
    FROM base
    WHERE rn = 1
    """
)

(
    patient_enrollments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollments_silver.csv")
)

# ======================================================================================
# Target: clinical_visits_silver
# ======================================================================================

clinical_visits_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(cvb.patient_id) AS patient_id,
            CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
            TRIM(cvb.visit_type) AS visit_type,
            TRIM(cvb.visit_id) AS practitioner_id,
            cvb.physician_notes AS notes,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(cvb.patient_id),
                    CAST(cvb.visit_date AS TIMESTAMP),
                    TRIM(cvb.visit_type)
                ORDER BY
                    TRIM(cvb.patient_id)
            ) AS rn
        FROM clinical_visit_bronze cvb
    )
    SELECT
        patient_id,
        visit_date,
        visit_type,
        practitioner_id,
        notes
    FROM base
    WHERE rn = 1
    """
)

(
    clinical_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_visits_silver.csv")
)

# ======================================================================================
# Target: lab_results_silver
# ======================================================================================

lab_results_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(lrb.patient_id) AS patient_id,
            CAST(lrb.test_date AS TIMESTAMP) AS test_date,
            TRIM(lrb.test_name) AS test_type,
            CAST(lrb.test_result AS DOUBLE) AS test_result,
            TRIM(lrb.test_unit) AS units,
            TRIM(lrb.reference_range) AS reference_range,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(lrb.patient_id),
                    CAST(lrb.test_date AS TIMESTAMP),
                    TRIM(lrb.test_name),
                    CAST(lrb.test_result AS DOUBLE),
                    TRIM(lrb.test_unit)
                ORDER BY
                    TRIM(lrb.patient_id)
            ) AS rn
        FROM lab_results_bronze lrb
    )
    SELECT
        patient_id,
        test_date,
        test_type,
        test_result,
        units,
        reference_range
    FROM base
    WHERE rn = 1
    """
)

(
    lab_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_silver.csv")
)

# ======================================================================================
# Target: drug_administrations_silver
# ======================================================================================

drug_administrations_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(dab.patient_id) AS patient_id,
            CAST(dab.administration_date AS TIMESTAMP) AS administration_date,
            TRIM(dab.drug_code) AS drug_name,
            CAST(dab.dosage_mg AS DOUBLE) AS dosage,
            TRIM(dab.administration_route) AS route,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(dab.patient_id),
                    CAST(dab.administration_date AS TIMESTAMP),
                    TRIM(dab.drug_code),
                    CAST(dab.dosage_mg AS DOUBLE),
                    TRIM(dab.administration_route)
                ORDER BY
                    TRIM(dab.patient_id)
            ) AS rn
        FROM drug_administration_bronze dab
    )
    SELECT
        patient_id,
        administration_date,
        drug_name,
        dosage,
        route
    FROM base
    WHERE rn = 1
    """
)

(
    drug_administrations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_administrations_silver.csv")
)

# ======================================================================================
# Target: adverse_events_silver
# ======================================================================================

adverse_events_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(aeb.patient_id) AS patient_id,
            CAST(aeb.event_start_date AS TIMESTAMP) AS event_date,
            TRIM(aeb.event_type) AS event_type,
            TRIM(aeb.severity) AS severity,
            TRIM(aeb.outcome) AS outcome,
            CAST(NULL AS STRING) AS notes,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(aeb.patient_id),
                    CAST(aeb.event_start_date AS TIMESTAMP),
                    TRIM(aeb.event_type),
                    TRIM(aeb.severity),
                    TRIM(aeb.outcome)
                ORDER BY
                    TRIM(aeb.patient_id)
            ) AS rn
        FROM adverse_events_bronze aeb
    )
    SELECT
        patient_id,
        event_date,
        event_type,
        severity,
        outcome,
        notes
    FROM base
    WHERE rn = 1
    """
)

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_events_silver.csv")
)

# ======================================================================================
# Target: wearable_metrics_silver
# ======================================================================================

wearable_metrics_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(wmb.patient_id) AS patient_id,
            CAST(wmb.recorded_timestamp AS TIMESTAMP) AS collection_date,
            TRIM(wmb.device_type) AS device_type,
            CAST(wmb.heart_rate AS FLOAT) AS heart_rate,
            CAST(wmb.step_count AS INT) AS steps,
            CAST(NULL AS DOUBLE) AS activity_duration,
            ROW_NUMBER() OVER (
                PARTITION BY
                    TRIM(wmb.patient_id),
                    CAST(wmb.recorded_timestamp AS TIMESTAMP),
                    TRIM(wmb.device_type)
                ORDER BY
                    TRIM(wmb.patient_id)
            ) AS rn
        FROM wearable_monitoring_bronze wmb
    )
    SELECT
        patient_id,
        collection_date,
        device_type,
        heart_rate,
        steps,
        activity_duration
    FROM base
    WHERE rn = 1
    """
)

(
    wearable_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_metrics_silver.csv")
)

job.commit()
