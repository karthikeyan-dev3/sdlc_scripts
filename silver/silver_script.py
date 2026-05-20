import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
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

# -----------------------------------
# patient_enrollment_silver (pes)
# -----------------------------------
patient_enrollment_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(peb.patient_id) AS patient_id,
        TRIM(peb.trial_id) AS trial_id,
        TRIM(peb.site_id) AS site_id,
        TRIM(peb.patient_name) AS patient_name,
        CASE
            WHEN UPPER(TRIM(peb.gender)) IN ('M', 'MALE') THEN 'MALE'
            WHEN UPPER(TRIM(peb.gender)) IN ('F', 'FEMALE') THEN 'FEMALE'
            ELSE UPPER(TRIM(peb.gender))
        END AS gender,
        CAST(peb.date_of_birth AS DATE) AS date_of_birth,
        TRIM(peb.country) AS country,
        CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
        TRIM(peb.consent_status) AS consent_status,
        TRIM(peb.source_system) AS source_system
    FROM patient_enrollment_bronze peb
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, trial_id, site_id, source_system
            ORDER BY enrollment_date DESC
        ) AS rn
    FROM base
)
SELECT
    patient_id,
    trial_id,
    site_id,
    patient_name,
    gender,
    date_of_birth,
    country,
    enrollment_date,
    consent_status,
    source_system
FROM dedup
WHERE rn = 1
"""
)

patient_enrollment_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/patient_enrollment_silver.csv")

# ------------------------------
# clinical_visit_silver (cvs)
# ------------------------------
clinical_visit_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(cvb.visit_id) AS visit_id,
        TRIM(cvb.patient_id) AS patient_id,
        TRIM(cvb.trial_id) AS trial_id,
        CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
        UPPER(TRIM(cvb.visit_type)) AS visit_type,
        CAST(cvb.blood_pressure AS STRING) AS blood_pressure,
        CASE
            WHEN CAST(cvb.heart_rate AS DOUBLE) < 0 OR CAST(cvb.heart_rate AS DOUBLE) > 300 THEN NULL
            ELSE CAST(cvb.heart_rate AS DOUBLE)
        END AS heart_rate,
        CASE
            WHEN CAST(cvb.weight_kg AS DOUBLE) < 0 OR CAST(cvb.weight_kg AS DOUBLE) > 1000 THEN NULL
            ELSE CAST(cvb.weight_kg AS DOUBLE)
        END AS weight_kg,
        TRIM(cvb.physician_notes) AS physician_notes,
        TRIM(cvb.source_system) AS source_system
    FROM clinical_visit_bronze cvb
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(visit_id, ''),
                CASE WHEN visit_id IS NULL OR TRIM(visit_id) = '' THEN patient_id ELSE '' END,
                CASE WHEN visit_id IS NULL OR TRIM(visit_id) = '' THEN trial_id ELSE '' END,
                CASE WHEN visit_id IS NULL OR TRIM(visit_id) = '' THEN CAST(visit_date AS STRING) ELSE '' END,
                CASE WHEN visit_id IS NULL OR TRIM(visit_id) = '' THEN visit_type ELSE '' END,
                CASE WHEN visit_id IS NULL OR TRIM(visit_id) = '' THEN source_system ELSE '' END
            ORDER BY visit_date DESC
        ) AS rn
    FROM base
)
SELECT
    visit_id,
    patient_id,
    trial_id,
    visit_date,
    visit_type,
    blood_pressure,
    heart_rate,
    weight_kg,
    physician_notes,
    source_system
FROM dedup
WHERE rn = 1
"""
)

clinical_visit_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/clinical_visit_silver.csv")

# --------------------------
# lab_results_silver (lrs)
# --------------------------
lab_results_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(lrb.lab_result_id) AS lab_result_id,
        TRIM(lrb.patient_id) AS patient_id,
        TRIM(lrb.sample_id) AS sample_id,
        TRIM(lrb.test_name) AS test_name,
        CAST(lrb.test_result AS DOUBLE) AS test_result,
        TRIM(lrb.test_unit) AS test_unit,
        TRIM(lrb.reference_range) AS reference_range,
        CASE
            WHEN UPPER(TRIM(lrb.abnormal_flag)) IN ('Y', 'YES', 'TRUE', 'T', '1', 'ABNORMAL') THEN 'Y'
            WHEN UPPER(TRIM(lrb.abnormal_flag)) IN ('N', 'NO', 'FALSE', 'F', '0', 'NORMAL') THEN 'N'
            ELSE UPPER(TRIM(lrb.abnormal_flag))
        END AS abnormal_flag,
        CAST(lrb.test_date AS TIMESTAMP) AS test_date,
        TRIM(lrb.lab_name) AS lab_name
    FROM lab_results_bronze lrb
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(lab_result_id, ''),
                CASE WHEN lab_result_id IS NULL OR TRIM(lab_result_id) = '' THEN patient_id ELSE '' END,
                CASE WHEN lab_result_id IS NULL OR TRIM(lab_result_id) = '' THEN sample_id ELSE '' END,
                CASE WHEN lab_result_id IS NULL OR TRIM(lab_result_id) = '' THEN test_name ELSE '' END,
                CASE WHEN lab_result_id IS NULL OR TRIM(lab_result_id) = '' THEN CAST(test_date AS STRING) ELSE '' END,
                CASE WHEN lab_result_id IS NULL OR TRIM(lab_result_id) = '' THEN lab_name ELSE '' END
            ORDER BY test_date DESC
        ) AS rn
    FROM base
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
FROM dedup
WHERE rn = 1
"""
)

lab_results_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/lab_results_silver.csv")

# --------------------------------
# drug_administration_silver (das)
# --------------------------------
drug_administration_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(dab.administration_id) AS administration_id,
        TRIM(dab.patient_id) AS patient_id,
        TRIM(dab.drug_code) AS drug_code,
        CAST(dab.dosage_mg AS DOUBLE) AS dosage_mg,
        CAST(dab.administration_date AS TIMESTAMP) AS administration_date,
        UPPER(TRIM(dab.administration_route)) AS administration_route,
        TRIM(dab.administered_by) AS administered_by,
        TRIM(dab.batch_number) AS batch_number
    FROM drug_administration_bronze dab
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(administration_id, ''),
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN patient_id ELSE '' END,
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN drug_code ELSE '' END,
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN CAST(dosage_mg AS STRING) ELSE '' END,
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN CAST(administration_date AS STRING) ELSE '' END,
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN administration_route ELSE '' END,
                CASE WHEN administration_id IS NULL OR TRIM(administration_id) = '' THEN batch_number ELSE '' END
            ORDER BY administration_date DESC
        ) AS rn
    FROM base
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
FROM dedup
WHERE rn = 1
"""
)

drug_administration_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/drug_administration_silver.csv")

# ----------------------------
# adverse_events_silver (aes)
# ----------------------------
adverse_events_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(aeb.event_id) AS event_id,
        TRIM(aeb.patient_id) AS patient_id,
        TRIM(aeb.event_type) AS event_type,
        CASE
            WHEN UPPER(TRIM(aeb.severity)) IN ('MILD', '1') THEN 'MILD'
            WHEN UPPER(TRIM(aeb.severity)) IN ('MODERATE', '2') THEN 'MODERATE'
            WHEN UPPER(TRIM(aeb.severity)) IN ('SEVERE', '3') THEN 'SEVERE'
            WHEN UPPER(TRIM(aeb.severity)) IN ('LIFE THREATENING', 'LIFE-THREATENING', '4') THEN 'LIFE THREATENING'
            WHEN UPPER(TRIM(aeb.severity)) IN ('FATAL', 'DEATH', '5') THEN 'FATAL'
            ELSE UPPER(TRIM(aeb.severity))
        END AS severity,
        CAST(aeb.event_start_date AS TIMESTAMP) AS event_start_date,
        CAST(aeb.event_end_date AS TIMESTAMP) AS event_end_date,
        TRIM(aeb.outcome) AS outcome,
        TRIM(aeb.related_to_drug) AS related_to_drug,
        TRIM(aeb.hospitalization_required) AS hospitalization_required,
        TRIM(aeb.reported_by) AS reported_by
    FROM adverse_events_bronze aeb
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(event_id, ''),
                CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN patient_id ELSE '' END,
                CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN event_type ELSE '' END,
                CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN CAST(event_start_date AS STRING) ELSE '' END,
                CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN CAST(event_end_date AS STRING) ELSE '' END,
                CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN severity ELSE '' END
            ORDER BY event_start_date DESC
        ) AS rn
    FROM base
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
    hospitalization_required,
    reported_by
FROM dedup
WHERE rn = 1
"""
)

adverse_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/adverse_events_silver.csv")

# ----------------------------------
# wearable_monitoring_silver (wms)
# ----------------------------------
wearable_monitoring_silver_df = spark.sql(
    """
WITH base AS (
    SELECT
        TRIM(wmb.device_record_id) AS device_record_id,
        TRIM(wmb.patient_id) AS patient_id,
        UPPER(TRIM(wmb.device_type)) AS device_type,
        CAST(wmb.recorded_timestamp AS TIMESTAMP) AS recorded_timestamp,
        CASE
            WHEN CAST(wmb.glucose_level AS DOUBLE) < 0 OR CAST(wmb.glucose_level AS DOUBLE) > 2000 THEN NULL
            ELSE CAST(wmb.glucose_level AS DOUBLE)
        END AS glucose_level,
        CASE
            WHEN CAST(wmb.step_count AS INT) < 0 OR CAST(wmb.step_count AS INT) > 1000000 THEN NULL
            ELSE CAST(wmb.step_count AS INT)
        END AS step_count,
        CASE
            WHEN CAST(wmb.sleep_hours AS DOUBLE) < 0 OR CAST(wmb.sleep_hours AS DOUBLE) > 24 THEN NULL
            ELSE CAST(wmb.sleep_hours AS DOUBLE)
        END AS sleep_hours,
        CASE
            WHEN CAST(wmb.heart_rate AS DOUBLE) < 0 OR CAST(wmb.heart_rate AS DOUBLE) > 300 THEN NULL
            ELSE CAST(wmb.heart_rate AS DOUBLE)
        END AS heart_rate,
        UPPER(TRIM(wmb.battery_status)) AS battery_status
    FROM wearable_monitoring_bronze wmb
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(device_record_id, ''),
                CASE WHEN device_record_id IS NULL OR TRIM(device_record_id) = '' THEN patient_id ELSE '' END,
                CASE WHEN device_record_id IS NULL OR TRIM(device_record_id) = '' THEN device_type ELSE '' END,
                CASE WHEN device_record_id IS NULL OR TRIM(device_record_id) = '' THEN CAST(recorded_timestamp AS STRING) ELSE '' END
            ORDER BY recorded_timestamp DESC
        ) AS rn
    FROM base
)
SELECT
    device_record_id,
    patient_id,
    device_type,
    recorded_timestamp,
    glucose_level,
    step_count,
    sleep_hours,
    heart_rate,
    battery_status
FROM dedup
WHERE rn = 1
"""
)

wearable_monitoring_silver_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "header", "true"
).save(f"{TARGET_PATH}/wearable_monitoring_silver.csv")
