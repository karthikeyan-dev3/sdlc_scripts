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

# ------------------------------------------------------------
# 1) Read source tables (S3) and create temp views
# ------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)
patient_silver_df.createOrReplaceTempView("patient_silver")

patient_visits_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_visits_silver.{FILE_FORMAT}/")
)
patient_visits_silver_df.createOrReplaceTempView("patient_visits_silver")

patient_lab_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_lab_results_silver.{FILE_FORMAT}/")
)
patient_lab_results_silver_df.createOrReplaceTempView("patient_lab_results_silver")

patient_drug_administrations_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_drug_administrations_silver.{FILE_FORMAT}/")
)
patient_drug_administrations_silver_df.createOrReplaceTempView("patient_drug_administrations_silver")

patient_adverse_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_adverse_events_silver.{FILE_FORMAT}/")
)
patient_adverse_events_silver_df.createOrReplaceTempView("patient_adverse_events_silver")

patient_wearable_observations_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_wearable_observations_silver.{FILE_FORMAT}/")
)
patient_wearable_observations_silver_df.createOrReplaceTempView("patient_wearable_observations_silver")

# ------------------------------------------------------------
# 2) gold_patient
# ------------------------------------------------------------
gold_patient_sql = """
SELECT
  CAST(ps.patient_id AS STRING)                      AS patient_id,
  CAST(ps.trial_id AS STRING)                        AS study_id,
  CAST(ps.site_id AS STRING)                         AS site_id,
  CAST(ps.gender AS STRING)                          AS gender,
  CAST(ps.date_of_birth AS DATE)                     AS birth_date,
  CAST(ps.country AS STRING)                         AS country,
  CAST(ps.enrollment_date AS TIMESTAMP)              AS enrollment_date,
  CAST(ps.consent_status AS STRING)                  AS consent_status
FROM patient_silver ps
"""
gold_patient_df = spark.sql(gold_patient_sql)

(
    gold_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient.csv")
)

# ------------------------------------------------------------
# 3) gold_patient_visit
# ------------------------------------------------------------
gold_patient_visit_sql = """
SELECT
  CAST(pvs.visit_id AS STRING)           AS visit_id,
  CAST(ps.trial_id AS STRING)            AS study_id,
  CAST(ps.site_id AS STRING)             AS site_id,
  CAST(pvs.visit_type AS STRING)         AS visit_type,
  CAST(pvs.visit_date AS TIMESTAMP)      AS visit_start_ts
FROM patient_visits_silver pvs
INNER JOIN patient_silver ps
  ON pvs.patient_id = ps.patient_id
 AND pvs.trial_id = ps.trial_id
"""
gold_patient_visit_df = spark.sql(gold_patient_visit_sql)

(
    gold_patient_visit_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_visit.csv")
)

# ------------------------------------------------------------
# 4) gold_patient_lab_result
# ------------------------------------------------------------
gold_patient_lab_result_sql = """
SELECT
  CAST(plrs.lab_result_id AS STRING)         AS lab_result_sk,
  CAST(ps.trial_id AS STRING)                AS study_id,
  CAST(ps.site_id AS STRING)                 AS site_id,
  CAST(plrs.sample_id AS STRING)             AS specimen_id,
  CAST(plrs.test_date AS TIMESTAMP)          AS result_ts,
  CAST(plrs.test_name AS STRING)             AS test_name,
  CAST(plrs.test_result AS DOUBLE)           AS result_value,
  CAST(plrs.test_unit AS STRING)             AS result_unit,
  CAST(plrs.reference_range AS STRING)       AS reference_range_low,
  CAST(plrs.reference_range AS STRING)       AS reference_range_high,
  CAST(plrs.abnormal_flag AS STRING)         AS abnormal_flag,
  CAST(plrs.lab_name AS STRING)              AS lab_name
FROM patient_lab_results_silver plrs
INNER JOIN patient_silver ps
  ON plrs.patient_id = ps.patient_id
 AND ps.trial_id = ps.trial_id
"""
gold_patient_lab_result_df = spark.sql(gold_patient_lab_result_sql)

(
    gold_patient_lab_result_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_lab_result.csv")
)

# ------------------------------------------------------------
# 5) gold_patient_drug_administration
# ------------------------------------------------------------
gold_patient_drug_administration_sql = """
SELECT
  CAST(pdas.administration_id AS STRING)         AS administration_id,
  CAST(ps.trial_id AS STRING)                    AS study_id,
  CAST(ps.site_id AS STRING)                     AS site_id,
  CAST(pdas.drug_code AS STRING)                 AS drug_code,
  CAST(pdas.dosage_mg AS DOUBLE)                 AS dose_value,
  CAST(pdas.administration_route AS STRING)      AS route,
  CAST(pdas.administration_date AS TIMESTAMP)    AS administration_start_ts,
  CAST(pdas.batch_number AS STRING)              AS lot_number,
  CAST(pdas.administered_by AS STRING)           AS administered_by
FROM patient_drug_administrations_silver pdas
INNER JOIN patient_silver ps
  ON pdas.patient_id = ps.patient_id
 AND ps.trial_id = ps.trial_id
"""
gold_patient_drug_administration_df = spark.sql(gold_patient_drug_administration_sql)

(
    gold_patient_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_drug_administration.csv")
)

# ------------------------------------------------------------
# 6) gold_patient_adverse_event
# ------------------------------------------------------------
gold_patient_adverse_event_sql = """
SELECT
  CAST(paes.event_id AS STRING)                AS adverse_event_id,
  CAST(ps.trial_id AS STRING)                  AS study_id,
  CAST(ps.site_id AS STRING)                   AS site_id,
  CAST(paes.event_type AS STRING)              AS ae_term,
  CAST(paes.severity AS STRING)                AS severity_grade,
  CAST(paes.related_to_drug AS BOOLEAN)        AS related_to_drug_flag,
  CAST(paes.outcome AS STRING)                 AS outcome,
  CAST(paes.event_start_date AS TIMESTAMP)     AS onset_ts,
  CAST(paes.event_end_date AS TIMESTAMP)       AS resolution_ts
FROM patient_adverse_events_silver paes
INNER JOIN patient_silver ps
  ON paes.patient_id = ps.patient_id
 AND ps.trial_id = ps.trial_id
"""
gold_patient_adverse_event_df = spark.sql(gold_patient_adverse_event_sql)

(
    gold_patient_adverse_event_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_adverse_event.csv")
)

# ------------------------------------------------------------
# 7) gold_patient_wearable_observation
# ------------------------------------------------------------
gold_patient_wearable_observation_sql = """
SELECT
  CAST(pwos.device_record_id AS STRING)         AS wearable_obs_sk,
  CAST(pwos.device_type AS STRING)              AS metric_name,
  CAST(pwos.recorded_timestamp AS TIMESTAMP)    AS observation_ts,
  CAST(pwos.step_count AS INT)                  AS metric_value
FROM patient_wearable_observations_silver pwos
INNER JOIN patient_silver ps
  ON pwos.patient_id = ps.patient_id
 AND ps.trial_id = ps.trial_id
"""
gold_patient_wearable_observation_df = spark.sql(gold_patient_wearable_observation_sql)

(
    gold_patient_wearable_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_wearable_observation.csv")
)

# ------------------------------------------------------------
# 8) gold_patient_360_daily
# ------------------------------------------------------------
gold_patient_360_daily_sql = """
SELECT
  CAST(ps.trial_id AS STRING)     AS study_id,
  CAST(ps.site_id AS STRING)      AS site_id
FROM patient_silver ps
INNER JOIN patient_visits_silver pvs
  ON pvs.patient_id = ps.patient_id
 AND pvs.trial_id = ps.trial_id
INNER JOIN patient_lab_results_silver plrs
  ON plrs.patient_id = ps.patient_id
INNER JOIN patient_drug_administrations_silver pdas
  ON pdas.patient_id = ps.patient_id
INNER JOIN patient_adverse_events_silver paes
  ON paes.patient_id = ps.patient_id
INNER JOIN patient_wearable_observations_silver pwos
  ON pwos.patient_id = ps.patient_id
"""
gold_patient_360_daily_df = spark.sql(gold_patient_360_daily_sql)

(
    gold_patient_360_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_360_daily.csv")
)

job.commit()
