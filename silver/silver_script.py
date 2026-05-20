import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables (Bronze)
# -----------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
cvb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
)
lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
dab_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
wmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
peb_df.createOrReplaceTempView("peb")
cvb_df.createOrReplaceTempView("cvb")
lrb_df.createOrReplaceTempView("lrb")
dab_df.createOrReplaceTempView("dab")
aeb_df.createOrReplaceTempView("aeb")
wmb_df.createOrReplaceTempView("wmb")

# ============================================================
# TABLE: silver.patient_identity_silver
# ============================================================
patient_identity_sql = """
WITH base AS (
  SELECT
    UPPER(TRIM(peb.patient_id)) AS patient_id,
    TRIM(peb.source_system) AS healthcare_system,
    TRIM(peb.trial_id) AS trial_id,
    TRIM(peb.site_id) AS site_id,
    CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
    TRIM(peb.consent_status) AS consent_status
  FROM peb
  WHERE peb.patient_id IS NOT NULL
),
dedup AS (
  SELECT
    patient_id,
    healthcare_system,
    trial_id,
    site_id,
    enrollment_date,
    consent_status,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id
      ORDER BY enrollment_date DESC NULLS LAST
    ) AS rn
  FROM base
)
SELECT
  patient_id,
  healthcare_system,
  trial_id,
  site_id,
  enrollment_date,
  consent_status
FROM dedup
WHERE rn = 1
"""
patient_identity_df = spark.sql(patient_identity_sql)

(
    patient_identity_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_identity_silver.csv")
)

patient_identity_df.createOrReplaceTempView("pis")

# ============================================================
# TABLE: silver.patient_clinical_events_silver
# NOTE: UDT does not define target columns for this table.
#       Per requirement, do not invent columns; write empty schema result.
# ============================================================
patient_clinical_events_sql = """
SELECT
  CAST(NULL AS STRING) AS patient_id
WHERE 1 = 0
"""
patient_clinical_events_df = spark.sql(patient_clinical_events_sql)

(
    patient_clinical_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_clinical_events_silver.csv")
)

patient_clinical_events_df.createOrReplaceTempView("pces")

# ============================================================
# TABLE: silver.patient_dashboard_metrics_silver
# NOTE: UDT does not define target columns for this table.
#       Per requirement, do not invent columns; write empty schema result.
# ============================================================
patient_dashboard_metrics_sql = """
SELECT
  CAST(NULL AS STRING) AS patient_id,
  CAST(NULL AS TIMESTAMP) AS metric_as_of_timestamp
WHERE 1 = 0
"""
patient_dashboard_metrics_df = spark.sql(patient_dashboard_metrics_sql)

(
    patient_dashboard_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_dashboard_metrics_silver.csv")
)

patient_dashboard_metrics_df.createOrReplaceTempView("pdms")

# ============================================================
# TABLE: silver.data_process_monitor_silver
# NOTE: UDT does not define target columns for this table.
#       Per requirement, do not invent columns; write empty schema result.
# ============================================================
data_process_monitor_sql = """
SELECT
  CAST(NULL AS TIMESTAMP) AS data_updated_timestamp
WHERE 1 = 0
"""
data_process_monitor_df = spark.sql(data_process_monitor_sql)

(
    data_process_monitor_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_process_monitor_silver.csv")
)

data_process_monitor_df.createOrReplaceTempView("dpms")

# ============================================================
# TABLE: silver.report_latency_silver
# NOTE: UDT does not define target columns for this table.
#       Per requirement, do not invent columns; write empty schema result.
# ============================================================
report_latency_sql = """
SELECT
  CAST(NULL AS TIMESTAMP) AS metric_as_of_timestamp,
  CAST(NULL AS TIMESTAMP) AS data_updated_timestamp
WHERE 1 = 0
"""
report_latency_df = spark.sql(report_latency_sql)

(
    report_latency_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/report_latency_silver.csv")
)

report_latency_df.createOrReplaceTempView("rls")

# ============================================================
# TABLE: silver.patient_analytics_silver
# NOTE: UDT does not define target columns for this table.
#       Per requirement, do not invent columns; write empty schema result.
# ============================================================
patient_analytics_sql = """
SELECT
  CAST(NULL AS STRING) AS patient_id
WHERE 1 = 0
"""
patient_analytics_df = spark.sql(patient_analytics_sql)

(
    patient_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_analytics_silver.csv")
)

job.commit()
