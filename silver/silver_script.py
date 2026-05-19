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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# Source Reads (Bronze)
# =========================
patients_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patients_bronze.{FILE_FORMAT}/")
)
patients_bronze_df.createOrReplaceTempView("patients_bronze")

health_metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/health_metrics_bronze.{FILE_FORMAT}/")
)
health_metrics_bronze_df.createOrReplaceTempView("health_metrics_bronze")

# =========================
# Target: silver.patients_silver
# =========================
patients_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.patient_id) AS STRING) AS patient_id,
    pb.trial_id AS trial_id,
    pb.site_id AS site_id,
    pb.gender AS gender,
    pb.date_of_birth AS date_of_birth,
    pb.country AS country,
    pb.enrollment_date AS enrollment_date,
    pb.consent_status AS consent_status,
    pb.source_system AS source_system,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.patient_id) AS STRING)
      ORDER BY pb.enrollment_date DESC
    ) AS rn
  FROM patients_bronze pb
),
dedup AS (
  SELECT
    patient_id,
    trial_id,
    site_id,
    gender,
    date_of_birth,
    country,
    enrollment_date,
    consent_status,
    source_system
  FROM base
  WHERE rn = 1
)
SELECT
  patient_id,
  trial_id,
  site_id,
  gender,
  date_of_birth,
  country,
  enrollment_date,
  consent_status,
  source_system
FROM dedup
"""
patients_silver_df = spark.sql(patients_silver_sql)
patients_silver_df.createOrReplaceTempView("patients_silver")

patients_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patients_silver.csv"
)

# =========================
# Target: silver.wearable_health_metrics_silver
# =========================
wearable_health_metrics_silver_sql = """
WITH joined AS (
  SELECT
    CAST(TRIM(hmb.patient_id) AS STRING) AS patient_id,
    hmb.recorded_timestamp AS timestamp,
    CAST(hmb.glucose_level AS DOUBLE) AS glucose_level,
    CAST(hmb.heart_rate AS DOUBLE) AS heart_rate,
    CAST(hmb.sleep_hours AS DOUBLE) AS sleep_duration,
    CAST(hmb.step_count AS DOUBLE) AS physical_activity,
    hmb.device_type AS device_type,
    hmb.device_record_id AS device_record_id,
    hmb.battery_status AS battery_status
  FROM health_metrics_bronze hmb
  INNER JOIN patients_silver ps
    ON ps.patient_id = CAST(TRIM(hmb.patient_id) AS STRING)
),
ranked AS (
  SELECT
    patient_id,
    timestamp,
    glucose_level,
    heart_rate,
    sleep_duration,
    physical_activity,
    device_type,
    device_record_id,
    battery_status,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, timestamp
      ORDER BY device_record_id DESC
    ) AS rn
  FROM joined
)
SELECT
  patient_id,
  timestamp,
  glucose_level,
  heart_rate,
  sleep_duration,
  physical_activity,
  device_type,
  device_record_id,
  battery_status
FROM ranked
WHERE rn = 1
"""
wearable_health_metrics_silver_df = spark.sql(wearable_health_metrics_silver_sql)
wearable_health_metrics_silver_df.createOrReplaceTempView("wearable_health_metrics_silver")

wearable_health_metrics_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/wearable_health_metrics_silver.csv"
)

# =========================
# Target: silver.health_risk_events_silver
# (Apply EXACT UDT column mappings provided)
# =========================
health_risk_events_silver_sql = """
SELECT
  whms.patient_id AS patient_id,
  CAST(whms.glucose_level AS STRING) AS health_risk_type,
  whms.timestamp AS risk_identified_at,
  CAST(whms.heart_rate AS STRING) AS risk_level
FROM wearable_health_metrics_silver whms
"""
health_risk_events_silver_df = spark.sql(health_risk_events_silver_sql)
health_risk_events_silver_df.createOrReplaceTempView("health_risk_events_silver")

health_risk_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/health_risk_events_silver.csv"
)

# =========================
# Target: silver.clinical_intervention_insights_silver
# (Apply EXACT UDT column mappings provided)
# =========================
clinical_intervention_insights_silver_sql = """
SELECT
  hres.patient_id AS patient_id,
  hres.health_risk_type AS insight_type,
  hres.risk_identified_at AS generated_at
FROM health_risk_events_silver hres
INNER JOIN wearable_health_metrics_silver whms
  ON whms.patient_id = hres.patient_id
 AND whms.timestamp = hres.risk_identified_at
"""
clinical_intervention_insights_silver_df = spark.sql(clinical_intervention_insights_silver_sql)

clinical_intervention_insights_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/clinical_intervention_insights_silver.csv"
)

job.commit()