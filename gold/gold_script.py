import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------
# 1) READ SOURCE TABLES FROM S3
# -------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)
patient_encounter_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_encounter_silver.{FILE_FORMAT}/")
)
lab_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_silver.{FILE_FORMAT}/")
)
drug_administration_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_silver.{FILE_FORMAT}/")
)
adverse_event_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_silver.{FILE_FORMAT}/")
)
wearable_observation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_observation_silver.{FILE_FORMAT}/")
)

# -------------------------------------
# 2) CREATE TEMP VIEWS
# -------------------------------------
patient_silver_df.createOrReplaceTempView("patient_silver")
patient_encounter_silver_df.createOrReplaceTempView("patient_encounter_silver")
lab_result_silver_df.createOrReplaceTempView("lab_result_silver")
drug_administration_silver_df.createOrReplaceTempView("drug_administration_silver")
adverse_event_silver_df.createOrReplaceTempView("adverse_event_silver")
wearable_observation_silver_df.createOrReplaceTempView("wearable_observation_silver")

# -------------------------------------
# TARGET: gold_patient
# -------------------------------------
gold_patient_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(ps.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(ps.date_of_birth AS DATE) AS date_of_birth,
    CAST(TRIM(ps.country) AS STRING) AS country,
    CAST(TRIM(ps.consent_status) AS STRING) AS consent_status,
    CAST(ps.enrollment_date AS TIMESTAMP) AS enrollment_date,
    CAST(TRIM(ps.patient_name) AS STRING) AS patient_name
  FROM patient_silver ps
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id
      ORDER BY enrollment_date DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  patient_id,
  site_id,
  trial_id,
  date_of_birth,
  country,
  consent_status,
  enrollment_date,
  CASE
    WHEN patient_name IS NOT NULL
         AND INSTR(TRIM(patient_name), ' ') > 0
         AND LENGTH(TRIM(patient_name)) > 0
    THEN TRIM(SUBSTRING(TRIM(patient_name), 1, INSTR(TRIM(patient_name), ' ') - 1))
    ELSE NULL
  END AS first_name,
  CASE
    WHEN patient_name IS NOT NULL
         AND INSTR(TRIM(patient_name), ' ') > 0
         AND LENGTH(TRIM(patient_name)) > 0
    THEN TRIM(SUBSTRING(TRIM(patient_name), INSTR(TRIM(patient_name), ' ') + 1))
    ELSE NULL
  END AS last_name
FROM dedup
WHERE rn = 1
"""
gold_patient_df = spark.sql(gold_patient_sql)

gold_patient_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_patient.csv"
)

# -------------------------------------
# TARGET: gold_patient_encounter
# -------------------------------------
gold_patient_encounter_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pes.visit_id) AS STRING) AS visit_id,
    CAST(TRIM(pes.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(pes.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(pes.visit_type) AS STRING) AS encounter_type,
    CAST(pes.visit_date AS TIMESTAMP) AS actual_start_ts,
    CAST(pes.weight_kg AS DOUBLE) AS weight_kg,
    CAST(pes.blood_pressure AS DOUBLE) AS blood_pressure_systolic,
    CAST(pes.heart_rate AS DOUBLE) AS heart_rate_bpm
  FROM patient_encounter_silver pes
  INNER JOIN patient_silver ps
    ON pes.patient_id = ps.patient_id
   AND pes.trial_id = ps.trial_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY visit_id
      ORDER BY actual_start_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  visit_id,
  patient_id,
  trial_id,
  site_id,
  encounter_type,
  actual_start_ts,
  weight_kg,
  blood_pressure_systolic,
  heart_rate_bpm
FROM dedup
WHERE rn = 1
"""
gold_patient_encounter_df = spark.sql(gold_patient_encounter_sql)

gold_patient_encounter_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_patient_encounter.csv"
)

# -------------------------------------
# TARGET: gold_lab_result
# -------------------------------------
gold_lab_result_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(lrs.lab_result_id) AS STRING) AS lab_result_id,
    CAST(TRIM(lrs.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(lrs.sample_id) AS STRING) AS specimen_id,
    CAST(TRIM(lrs.test_name) AS STRING) AS test_name,
    CAST(lrs.test_result AS DOUBLE) AS result_value,
    CAST(TRIM(lrs.test_unit) AS STRING) AS result_unit,
    CAST(TRIM(lrs.reference_range) AS STRING) AS reference_range_low,
    CAST(TRIM(lrs.reference_range) AS STRING) AS reference_range_high,
    CAST(TRIM(lrs.abnormal_flag) AS STRING) AS abnormal_flag,
    CAST(lrs.test_date AS TIMESTAMP) AS result_ts,
    CAST(TRIM(lrs.lab_name) AS STRING) AS lab_name
  FROM lab_result_silver lrs
  INNER JOIN patient_silver ps
    ON lrs.patient_id = ps.patient_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY lab_result_id
      ORDER BY result_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  lab_result_id,
  patient_id,
  site_id,
  trial_id,
  specimen_id,
  test_name,
  result_value,
  result_unit,
  reference_range_low,
  reference_range_high,
  abnormal_flag,
  result_ts,
  lab_name
FROM dedup
WHERE rn = 1
"""
gold_lab_result_df = spark.sql(gold_lab_result_sql)

gold_lab_result_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_lab_result.csv"
)

# -------------------------------------
# TARGET: gold_drug_administration
# -------------------------------------
gold_drug_administration_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(das.administration_id) AS STRING) AS drug_admin_id,
    CAST(TRIM(das.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(das.drug_code) AS STRING) AS drug_code,
    CAST(das.dosage_mg AS FLOAT) AS dose,
    CAST(TRIM(das.administration_route) AS STRING) AS route,
    CAST(das.administration_date AS TIMESTAMP) AS administration_start_ts,
    CAST(TRIM(das.batch_number) AS STRING) AS lot_number
  FROM drug_administration_silver das
  INNER JOIN patient_silver ps
    ON das.patient_id = ps.patient_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY drug_admin_id
      ORDER BY administration_start_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  drug_admin_id,
  patient_id,
  site_id,
  trial_id,
  drug_code,
  dose,
  route,
  administration_start_ts,
  lot_number
FROM dedup
WHERE rn = 1
"""
gold_drug_administration_df = spark.sql(gold_drug_administration_sql)

gold_drug_administration_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_drug_administration.csv"
)

# -------------------------------------
# TARGET: gold_adverse_event
# -------------------------------------
gold_adverse_event_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(aes.event_id) AS STRING) AS adverse_event_id,
    CAST(TRIM(aes.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(aes.event_type) AS STRING) AS event_term,
    CAST(TRIM(aes.severity) AS STRING) AS severity_grade,
    CAST(aes.related_to_drug AS BOOLEAN) AS related_to_drug_flag,
    CAST(TRIM(aes.outcome) AS STRING) AS outcome,
    CAST(aes.event_start_date AS TIMESTAMP) AS event_start_ts,
    CAST(aes.event_end_date AS TIMESTAMP) AS event_end_ts,
    CAST(TRIM(aes.reported_by) AS STRING) AS reported_by
  FROM adverse_event_silver aes
  INNER JOIN patient_silver ps
    ON aes.patient_id = ps.patient_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY adverse_event_id
      ORDER BY event_start_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  adverse_event_id,
  patient_id,
  site_id,
  trial_id,
  event_term,
  severity_grade,
  related_to_drug_flag,
  outcome,
  event_start_ts,
  event_end_ts,
  reported_by
FROM dedup
WHERE rn = 1
"""
gold_adverse_event_df = spark.sql(gold_adverse_event_sql)

gold_adverse_event_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_adverse_event.csv"
)

# -------------------------------------
# TARGET: gold_wearable_observation
# -------------------------------------
gold_wearable_observation_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(wos.device_record_id) AS STRING) AS wearable_obs_id,
    CAST(TRIM(wos.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(wos.device_type) AS STRING) AS source_device_id,
    CAST(wos.recorded_timestamp AS TIMESTAMP) AS measurement_ts,
    CAST(wos.glucose_level AS DOUBLE) AS metric_value
  FROM wearable_observation_silver wos
  INNER JOIN patient_silver ps
    ON wos.patient_id = ps.patient_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY wearable_obs_id
      ORDER BY measurement_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  wearable_obs_id,
  patient_id,
  site_id,
  trial_id,
  source_device_id,
  measurement_ts,
  metric_value
FROM dedup
WHERE rn = 1
"""
gold_wearable_observation_df = spark.sql(gold_wearable_observation_sql)

gold_wearable_observation_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_wearable_observation.csv"
)

# -------------------------------------
# TARGET: gold_patient_360_timeline
# Note: Output columns limited strictly to those provided in UDT "columns" list for gpt.
# -------------------------------------
gold_patient_360_timeline_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(ps.patient_id) AS STRING) AS patient_id,
    CAST(TRIM(ps.trial_id) AS STRING) AS trial_id,
    CAST(TRIM(ps.site_id) AS STRING) AS site_id,
    CAST(TRIM(pes.visit_id) AS STRING) AS activity_id,
    CAST(TRIM(pes.visit_type) AS STRING) AS activity_type,
    CAST(pes.visit_date AS TIMESTAMP) AS activity_ts,
    CAST(TRIM(aes.severity) AS STRING) AS severity_grade,
    CAST(lrs.test_result AS DOUBLE) AS result_value,
    CAST(TRIM(lrs.test_unit) AS STRING) AS result_unit
  FROM patient_silver ps
  LEFT JOIN patient_encounter_silver pes
    ON pes.patient_id = ps.patient_id
   AND pes.trial_id = ps.trial_id
  LEFT JOIN lab_result_silver lrs
    ON lrs.patient_id = ps.patient_id
  LEFT JOIN drug_administration_silver das
    ON das.patient_id = ps.patient_id
  LEFT JOIN adverse_event_silver aes
    ON aes.patient_id = ps.patient_id
  LEFT JOIN wearable_observation_silver wos
    ON wos.patient_id = ps.patient_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, activity_id
      ORDER BY activity_ts DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  patient_id,
  trial_id,
  site_id,
  activity_id,
  activity_type,
  activity_ts,
  severity_grade,
  result_value,
  result_unit
FROM dedup
WHERE rn = 1
"""
gold_patient_360_timeline_df = spark.sql(gold_patient_360_timeline_sql)

gold_patient_360_timeline_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_patient_360_timeline.csv"
)

job.commit()