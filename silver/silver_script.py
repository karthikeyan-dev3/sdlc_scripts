import sys
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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read source tables from S3 (Bronze) and create temp views
# ------------------------------------------------------------------------------

patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)
patient_bronze_df.createOrReplaceTempView("patient_bronze")

patient_visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_visits_bronze.{FILE_FORMAT}/")
)
patient_visits_bronze_df.createOrReplaceTempView("patient_visits_bronze")

patient_lab_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_lab_results_bronze.{FILE_FORMAT}/")
)
patient_lab_results_bronze_df.createOrReplaceTempView("patient_lab_results_bronze")

patient_drug_administrations_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_drug_administrations_bronze.{FILE_FORMAT}/")
)
patient_drug_administrations_bronze_df.createOrReplaceTempView("patient_drug_administrations_bronze")

patient_adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_adverse_events_bronze.{FILE_FORMAT}/")
)
patient_adverse_events_bronze_df.createOrReplaceTempView("patient_adverse_events_bronze")

patient_wearable_observations_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_wearable_observations_bronze.{FILE_FORMAT}/")
)
patient_wearable_observations_bronze_df.createOrReplaceTempView("patient_wearable_observations_bronze")

# ------------------------------------------------------------------------------
# Target: silver.patient_silver
# ------------------------------------------------------------------------------

patient_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.patient_id AS STRING)          AS patient_id,
            CAST(pb.trial_id AS STRING)            AS trial_id,
            CAST(pb.site_id AS STRING)             AS site_id,
            CAST(pb.gender AS STRING)              AS gender,
            CAST(pb.date_of_birth AS DATE)         AS date_of_birth,
            CAST(pb.country AS STRING)             AS country,
            CAST(pb.enrollment_date AS TIMESTAMP)  AS enrollment_date,
            CAST(pb.consent_status AS STRING)      AS consent_status,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pb.patient_id AS STRING)
                ORDER BY CAST(pb.patient_id AS STRING)
            ) AS rn
        FROM patient_bronze pb
    )
    SELECT
        patient_id,
        trial_id,
        site_id,
        gender,
        date_of_birth,
        country,
        enrollment_date,
        consent_status
    FROM base
    WHERE rn = 1
    """
)

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.patient_visits_silver
# ------------------------------------------------------------------------------

patient_visits_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pvb.visit_id AS STRING)        AS visit_id,
            CAST(pvb.patient_id AS STRING)      AS patient_id,
            CAST(pvb.trial_id AS STRING)        AS trial_id,
            CAST(pvb.visit_date AS TIMESTAMP)   AS visit_date,
            CAST(pvb.visit_type AS STRING)      AS visit_type,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pvb.visit_id AS STRING)
                ORDER BY CAST(pvb.visit_id AS STRING)
            ) AS rn
        FROM patient_visits_bronze pvb
    )
    SELECT
        visit_id,
        patient_id,
        trial_id,
        visit_date,
        visit_type
    FROM base
    WHERE rn = 1
    """
)

(
    patient_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_visits_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.patient_lab_results_silver
# ------------------------------------------------------------------------------

patient_lab_results_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(plrb.lab_result_id AS STRING)     AS lab_result_id,
            CAST(plrb.patient_id AS STRING)        AS patient_id,
            CAST(plrb.sample_id AS STRING)         AS sample_id,
            CAST(plrb.test_name AS STRING)         AS test_name,
            CAST(plrb.test_result AS DOUBLE)       AS test_result,
            CAST(plrb.test_unit AS STRING)         AS test_unit,
            CAST(plrb.reference_range AS STRING)   AS reference_range,
            CAST(plrb.abnormal_flag AS STRING)     AS abnormal_flag,
            CAST(plrb.test_date AS TIMESTAMP)      AS test_date,
            CAST(plrb.lab_name AS STRING)          AS lab_name,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(plrb.lab_result_id AS STRING)
                ORDER BY CAST(plrb.lab_result_id AS STRING)
            ) AS rn
        FROM patient_lab_results_bronze plrb
    )
    SELECT
        lab_result_id,
        patient_id,
        sample_id,
        test_name,
        test_result,
        test_unit,
        reference_range,
        abnormal_flag,
        test_date,
        lab_name
    FROM base
    WHERE rn = 1
    """
)

(
    patient_lab_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_lab_results_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.patient_drug_administrations_silver
# ------------------------------------------------------------------------------

patient_drug_administrations_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pdab.administration_id AS STRING)       AS administration_id,
            CAST(pdab.patient_id AS STRING)              AS patient_id,
            CAST(pdab.drug_code AS STRING)               AS drug_code,
            CAST(pdab.dosage_mg AS DOUBLE)               AS dosage_mg,
            CAST(pdab.administration_date AS TIMESTAMP)  AS administration_date,
            CAST(pdab.administration_route AS STRING)    AS administration_route,
            CAST(pdab.administered_by AS STRING)         AS administered_by,
            CAST(pdab.batch_number AS STRING)            AS batch_number,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pdab.administration_id AS STRING)
                ORDER BY CAST(pdab.administration_id AS STRING)
            ) AS rn
        FROM patient_drug_administrations_bronze pdab
    )
    SELECT
        administration_id,
        patient_id,
        drug_code,
        dosage_mg,
        administration_date,
        administration_route,
        administered_by,
        batch_number
    FROM base
    WHERE rn = 1
    """
)

(
    patient_drug_administrations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_drug_administrations_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.patient_adverse_events_silver
# ------------------------------------------------------------------------------

patient_adverse_events_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(paeb.event_id AS STRING)                 AS event_id,
            CAST(paeb.patient_id AS STRING)               AS patient_id,
            CAST(paeb.event_type AS STRING)               AS event_type,
            CAST(paeb.severity AS STRING)                 AS severity,
            CAST(paeb.event_start_date AS TIMESTAMP)      AS event_start_date,
            CAST(paeb.event_end_date AS TIMESTAMP)        AS event_end_date,
            CAST(paeb.outcome AS STRING)                  AS outcome,
            CAST(paeb.related_to_drug AS BOOLEAN)         AS related_to_drug,
            CAST(paeb.hospitalization_required AS BOOLEAN) AS hospitalization_required,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(paeb.event_id AS STRING)
                ORDER BY CAST(paeb.event_id AS STRING)
            ) AS rn
        FROM patient_adverse_events_bronze paeb
    )
    SELECT
        event_id,
        patient_id,
        event_type,
        severity,
        event_start_date,
        event_end_date,
        outcome,
        related_to_drug,
        hospitalization_required
    FROM base
    WHERE rn = 1
    """
)

(
    patient_adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_adverse_events_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.patient_wearable_observations_silver
# ------------------------------------------------------------------------------

patient_wearable_observations_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pwob.device_record_id AS STRING)       AS device_record_id,
            CAST(pwob.patient_id AS STRING)             AS patient_id,
            CAST(pwob.device_type AS STRING)            AS device_type,
            CAST(pwob.recorded_timestamp AS TIMESTAMP)  AS recorded_timestamp,
            CAST(pwob.step_count AS INT)                AS step_count,
            CAST(pwob.heart_rate AS FLOAT)              AS heart_rate,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pwob.device_record_id AS STRING)
                ORDER BY CAST(pwob.device_record_id AS STRING)
            ) AS rn
        FROM patient_wearable_observations_bronze pwob
    )
    SELECT
        device_record_id,
        patient_id,
        device_type,
        recorded_timestamp,
        step_count,
        heart_rate
    FROM base
    WHERE rn = 1
    """
)

(
    patient_wearable_observations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_wearable_observations_silver.csv")
)

job.commit()