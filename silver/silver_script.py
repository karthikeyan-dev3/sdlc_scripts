import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

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
# Read Source Tables from S3 (Bronze)
# ------------------------------------------------------------------------------

patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)

patient_encounter_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_encounter_bronze.{FILE_FORMAT}/")
)

lab_result_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_bronze.{FILE_FORMAT}/")
)

drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)

adverse_event_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_bronze.{FILE_FORMAT}/")
)

wearable_observation_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_observation_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------------------

patient_bronze_df.createOrReplaceTempView("patient_bronze")
patient_encounter_bronze_df.createOrReplaceTempView("patient_encounter_bronze")
lab_result_bronze_df.createOrReplaceTempView("lab_result_bronze")
drug_administration_bronze_df.createOrReplaceTempView("drug_administration_bronze")
adverse_event_bronze_df.createOrReplaceTempView("adverse_event_bronze")
wearable_observation_bronze_df.createOrReplaceTempView("wearable_observation_bronze")

# ------------------------------------------------------------------------------
# Target: silver.patient_silver
# ------------------------------------------------------------------------------

patient_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.patient_id) AS STRING) AS patient_id,
        CAST(TRIM(pb.trial_id) AS STRING) AS trial_id,
        CAST(TRIM(pb.site_id) AS STRING) AS site_id,
        CAST(NULLIF(TRIM(pb.patient_name), '') AS STRING) AS patient_name,
        CAST(NULLIF(UPPER(TRIM(pb.gender)), '') AS STRING) AS gender,
        CAST(DATE(TRIM(pb.date_of_birth)) AS DATE) AS date_of_birth,
        CAST(NULLIF(TRIM(pb.country), '') AS STRING) AS country,
        CAST(pb.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(NULLIF(TRIM(pb.consent_status), '') AS STRING) AS consent_status,
        CAST(NULLIF(TRIM(pb.source_system), '') AS STRING) AS source_system
      FROM patient_bronze pb
    ),
    ranked AS (
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
    FROM ranked
    WHERE rn = 1
    """
)

patient_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_silver.csv"
)

# ------------------------------------------------------------------------------
# Target: silver.patient_encounter_silver
# ------------------------------------------------------------------------------

patient_encounter_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(peb.visit_id) AS STRING) AS visit_id,
        CAST(TRIM(peb.patient_id) AS STRING) AS patient_id,
        CAST(TRIM(peb.trial_id) AS STRING) AS trial_id,
        CAST(peb.visit_date AS TIMESTAMP) AS visit_date,
        CAST(NULLIF(LOWER(TRIM(peb.visit_type)), '') AS STRING) AS visit_type,
        CAST(peb.blood_pressure AS DOUBLE) AS blood_pressure,
        CAST(peb.heart_rate AS DOUBLE) AS heart_rate,
        CAST(peb.weight_kg AS DOUBLE) AS weight_kg,
        CAST(peb.physician_notes AS STRING) AS physician_notes,
        CAST(NULLIF(TRIM(peb.source_system), '') AS STRING) AS source_system
      FROM patient_encounter_bronze peb
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY visit_id, patient_id, trial_id, source_system
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
    FROM ranked
    WHERE rn = 1
    """
)

patient_encounter_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_encounter_silver.csv"
)

# ------------------------------------------------------------------------------
# Target: silver.lab_result_silver
# ------------------------------------------------------------------------------

lab_result_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(lrb.lab_result_id) AS STRING) AS lab_result_id,
        CAST(TRIM(lrb.patient_id) AS STRING) AS patient_id,
        CAST(TRIM(lrb.sample_id) AS STRING) AS sample_id,
        CAST(NULLIF(LOWER(TRIM(lrb.test_name)), '') AS STRING) AS test_name,
        CAST(lrb.test_result AS DOUBLE) AS test_result,
        CAST(NULLIF(LOWER(TRIM(lrb.test_unit)), '') AS STRING) AS test_unit,
        CAST(NULLIF(TRIM(lrb.reference_range), '') AS STRING) AS reference_range,
        CAST(NULLIF(UPPER(TRIM(lrb.abnormal_flag)), '') AS STRING) AS abnormal_flag,
        CAST(lrb.test_date AS TIMESTAMP) AS test_date,
        CAST(NULLIF(TRIM(lrb.lab_name), '') AS STRING) AS lab_name
      FROM lab_result_bronze lrb
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY lab_result_id, patient_id
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
    FROM ranked
    WHERE rn = 1
    """
)

lab_result_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/lab_result_silver.csv"
)

# ------------------------------------------------------------------------------
# Target: silver.drug_administration_silver
# ------------------------------------------------------------------------------

drug_administration_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(dab.administration_id) AS STRING) AS administration_id,
        CAST(TRIM(dab.patient_id) AS STRING) AS patient_id,
        CAST(TRIM(dab.drug_code) AS STRING) AS drug_code,
        CAST(dab.dosage_mg AS FLOAT) AS dosage_mg,
        CAST(dab.administration_date AS TIMESTAMP) AS administration_date,
        CAST(NULLIF(LOWER(TRIM(dab.administration_route)), '') AS STRING) AS administration_route,
        CAST(TRIM(dab.administered_by) AS STRING) AS administered_by,
        CAST(TRIM(dab.batch_number) AS STRING) AS batch_number
      FROM drug_administration_bronze dab
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY administration_id, patient_id
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
    FROM ranked
    WHERE rn = 1
    """
)

drug_administration_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/drug_administration_silver.csv"
)

# ------------------------------------------------------------------------------
# Target: silver.adverse_event_silver
# ------------------------------------------------------------------------------

adverse_event_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(aeb.event_id) AS STRING) AS event_id,
        CAST(TRIM(aeb.patient_id) AS STRING) AS patient_id,
        CAST(NULLIF(LOWER(TRIM(aeb.event_type)), '') AS STRING) AS event_type,
        CAST(NULLIF(LOWER(TRIM(aeb.severity)), '') AS STRING) AS severity,
        CAST(aeb.event_start_date AS TIMESTAMP) AS event_start_date,
        CAST(aeb.event_end_date AS TIMESTAMP) AS event_end_date,
        CAST(NULLIF(TRIM(aeb.outcome), '') AS STRING) AS outcome,
        CAST(aeb.related_to_drug AS BOOLEAN) AS related_to_drug,
        CAST(aeb.hospitalization_required AS BOOLEAN) AS hospitalization_required,
        CAST(NULLIF(TRIM(aeb.reported_by), '') AS STRING) AS reported_by
      FROM adverse_event_bronze aeb
    ),
    normalized AS (
      SELECT
        event_id,
        patient_id,
        event_type,
        severity,
        event_start_date,
        CASE
          WHEN event_end_date IS NOT NULL
               AND event_start_date IS NOT NULL
               AND event_end_date < event_start_date
            THEN event_start_date
          ELSE event_end_date
        END AS event_end_date,
        outcome,
        related_to_drug,
        hospitalization_required,
        reported_by
      FROM base
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY event_id, patient_id
          ORDER BY event_start_date DESC
        ) AS rn
      FROM normalized
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
    FROM ranked
    WHERE rn = 1
    """
)

adverse_event_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/adverse_event_silver.csv"
)

# ------------------------------------------------------------------------------
# Target: silver.wearable_observation_silver
# ------------------------------------------------------------------------------

wearable_observation_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(wob.device_record_id) AS STRING) AS device_record_id,
        CAST(TRIM(wob.patient_id) AS STRING) AS patient_id,
        CAST(NULLIF(LOWER(TRIM(wob.device_type)), '') AS STRING) AS device_type,
        CAST(wob.recorded_timestamp AS TIMESTAMP) AS recorded_timestamp,
        CAST(wob.glucose_level AS DOUBLE) AS glucose_level,
        CAST(wob.step_count AS INT) AS step_count,
        CAST(wob.sleep_hours AS DOUBLE) AS sleep_hours,
        CAST(wob.heart_rate AS DOUBLE) AS heart_rate,
        CAST(NULLIF(LOWER(TRIM(wob.battery_status)), '') AS STRING) AS battery_status
      FROM wearable_observation_bronze wob
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY device_record_id, patient_id
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
    FROM ranked
    WHERE rn = 1
    """
)

wearable_observation_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/wearable_observation_silver.csv"
)

job.commit()