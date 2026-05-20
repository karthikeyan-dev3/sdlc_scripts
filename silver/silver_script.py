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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables (S3 -> DataFrames)
# ------------------------------------------------------------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)

lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)

aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)

wddb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_device_data_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
peb_df.createOrReplaceTempView("patient_enrollment_bronze")
lrb_df.createOrReplaceTempView("lab_results_bronze")
aeb_df.createOrReplaceTempView("adverse_events_bronze")
wddb_df.createOrReplaceTempView("wearable_device_data_bronze")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Write outputs (each target separately)
# ------------------------------------------------------------------------------

# ---------------------------
# patient_enrollment_silver
# ---------------------------
patient_enrollment_silver_df = spark.sql("""
WITH base AS (
  SELECT
    SHA2(
      CONCAT(
        TRIM(UPPER(peb.patient_id)), '|',
        TRIM(UPPER(peb.trial_id)), '|',
        CAST(CAST(peb.enrollment_date AS timestamp) AS string)
      ),
      256
    ) AS enrollment_id,
    TRIM(UPPER(peb.patient_id)) AS patient_id,
    TRIM(UPPER(peb.trial_id)) AS trial_id,
    CAST(peb.enrollment_date AS timestamp) AS enrollment_date,
    CASE
      WHEN UPPER(TRIM(peb.consent_status)) IN ('CONSENTED','ENROLLED') THEN 'ENROLLED'
      WHEN UPPER(TRIM(peb.consent_status)) IN ('WITHDRAWN','DECLINED') THEN 'WITHDRAWN'
      ELSE 'PENDING'
    END AS status
  FROM patient_enrollment_bronze peb
),
dedup AS (
  SELECT
    enrollment_id,
    patient_id,
    trial_id,
    enrollment_date,
    status,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, trial_id, enrollment_date
      ORDER BY enrollment_date DESC
    ) AS rn
  FROM base
)
SELECT
  enrollment_id,
  patient_id,
  trial_id,
  enrollment_date,
  status
FROM dedup
WHERE rn = 1
""")

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollment_silver.csv")
)

# ---------------------------
# lab_results_silver
# ---------------------------
lab_results_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    lrb.lab_result_id AS result_id,
    TRIM(UPPER(lrb.patient_id)) AS patient_id,
    TRIM(UPPER(peb.trial_id)) AS trial_id,
    lrb.test_date AS result_date,
    TRIM(UPPER(lrb.test_name)) AS test_type,
    lrb.test_result AS result_value,
    TRIM(UPPER(lrb.test_unit)) AS unit,
    CASE
      WHEN UPPER(TRIM(lrb.abnormal_flag)) IN ('H','HIGH','ABNORMAL') THEN 'ABNORMAL'
      WHEN UPPER(TRIM(lrb.abnormal_flag)) IN ('N','NORMAL') THEN 'NORMAL'
      ELSE 'UNKNOWN'
    END AS status
  FROM lab_results_bronze lrb
  LEFT JOIN patient_enrollment_bronze peb
    ON lrb.patient_id = peb.patient_id
),
dedup AS (
  SELECT
    result_id,
    patient_id,
    trial_id,
    result_date,
    test_type,
    result_value,
    unit,
    status,
    ROW_NUMBER() OVER (
      PARTITION BY result_id
      ORDER BY result_date DESC
    ) AS rn
  FROM joined
)
SELECT
  result_id,
  patient_id,
  trial_id,
  result_date,
  test_type,
  result_value,
  unit,
  status
FROM dedup
WHERE rn = 1
""")

(
    lab_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_silver.csv")
)

# ---------------------------
# adverse_events_silver
# ---------------------------
adverse_events_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    aeb.event_id AS event_id,
    TRIM(UPPER(aeb.patient_id)) AS patient_id,
    TRIM(UPPER(peb.trial_id)) AS trial_id,
    COALESCE(aeb.event_start_date, aeb.event_end_date) AS event_date,
    TRIM(UPPER(aeb.event_type)) AS event_type,
    CASE
      WHEN UPPER(TRIM(aeb.severity)) IN ('MILD','MODERATE','SEVERE') THEN UPPER(TRIM(aeb.severity))
      ELSE 'UNKNOWN'
    END AS severity,
    aeb.related_to_drug AS related_to_study_drug
  FROM adverse_events_bronze aeb
  LEFT JOIN patient_enrollment_bronze peb
    ON aeb.patient_id = peb.patient_id
),
dedup AS (
  SELECT
    event_id,
    patient_id,
    trial_id,
    event_date,
    event_type,
    severity,
    related_to_study_drug,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY event_date DESC
    ) AS rn
  FROM joined
)
SELECT
  event_id,
  patient_id,
  trial_id,
  event_date,
  event_type,
  severity,
  related_to_study_drug
FROM dedup
WHERE rn = 1
""")

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_events_silver.csv")
)

# ---------------------------
# wearable_device_data_silver
# ---------------------------
wearable_device_data_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    wddb.device_record_id AS device_id,
    TRIM(UPPER(wddb.patient_id)) AS patient_id,
    TRIM(UPPER(peb.trial_id)) AS trial_id,
    wddb.recorded_timestamp AS measurement_date,
    wddb.glucose_level AS metric_value,
    'glucose_level' AS metric_type,
    'mg/dL' AS unit
  FROM wearable_device_data_bronze wddb
  LEFT JOIN patient_enrollment_bronze peb
    ON wddb.patient_id = peb.patient_id
),
filtered AS (
  SELECT
    device_id,
    patient_id,
    trial_id,
    measurement_date,
    metric_type,
    metric_value,
    unit
  FROM joined
  WHERE metric_value IS NOT NULL
),
dedup AS (
  SELECT
    device_id,
    patient_id,
    trial_id,
    measurement_date,
    metric_type,
    metric_value,
    unit,
    ROW_NUMBER() OVER (
      PARTITION BY device_id, metric_type, measurement_date
      ORDER BY measurement_date DESC
    ) AS rn
  FROM filtered
)
SELECT
  device_id,
  patient_id,
  trial_id,
  measurement_date,
  metric_type,
  metric_value,
  unit
FROM dedup
WHERE rn = 1
""")

(
    wearable_device_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_device_data_silver.csv")
)

job.commit()