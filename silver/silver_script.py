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

# --------------------------------------------------------------------
# Read source tables (Bronze) and create temp views
# --------------------------------------------------------------------
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

# --------------------------------------------------------------------
# Target: clinical_trials_silver
# --------------------------------------------------------------------
clinical_trials_silver_sql = """
WITH base AS (
  SELECT
    peb.trial_id AS trial_id,
    MIN(CAST(peb.enrollment_date AS DATE)) OVER (PARTITION BY peb.trial_id) AS start_date,
    MAX(peb.source_system) OVER (PARTITION BY peb.trial_id) AS source_system,
    ROW_NUMBER() OVER (PARTITION BY peb.trial_id ORDER BY peb.enrollment_date DESC) AS rn
  FROM patient_enrollment_bronze peb
)
SELECT
  trial_id,
  start_date,
  source_system
FROM base
WHERE rn = 1
"""
clinical_trials_silver_df = spark.sql(clinical_trials_silver_sql)
clinical_trials_silver_df.createOrReplaceTempView("clinical_trials_silver")

(
    clinical_trials_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trials_silver.csv")
)

# --------------------------------------------------------------------
# Target: audit_trail_silver
# --------------------------------------------------------------------
audit_trail_silver_sql = """
WITH unioned AS (
  SELECT
    peb.trial_id AS business_key,
    peb.enrollment_date AS change_timestamp,
    peb.source_system AS changed_by,
    'patient_enrollment_bronze' AS source_table
  FROM patient_enrollment_bronze peb

  UNION ALL

  SELECT
    cvb.visit_id AS business_key,
    cvb.visit_date AS change_timestamp,
    cvb.source_system AS changed_by,
    'clinical_visit_bronze' AS source_table
  FROM clinical_visit_bronze cvb

  UNION ALL

  SELECT
    lrb.lab_result_id AS business_key,
    lrb.test_date AS change_timestamp,
    lrb.lab_name AS changed_by,
    'lab_results_bronze' AS source_table
  FROM lab_results_bronze lrb

  UNION ALL

  SELECT
    dab.administration_id AS business_key,
    dab.administration_date AS change_timestamp,
    dab.administered_by AS changed_by,
    'drug_administration_bronze' AS source_table
  FROM drug_administration_bronze dab

  UNION ALL

  SELECT
    aeb.event_id AS business_key,
    aeb.event_start_date AS change_timestamp,
    aeb.reported_by AS changed_by,
    'adverse_events_bronze' AS source_table
  FROM adverse_events_bronze aeb

  UNION ALL

  SELECT
    wmb.device_record_id AS business_key,
    wmb.recorded_timestamp AS change_timestamp,
    wmb.device_type AS changed_by,
    'wearable_monitoring_bronze' AS source_table
  FROM wearable_monitoring_bronze wmb
),
dedup AS (
  SELECT
    business_key,
    change_timestamp,
    changed_by,
    source_table,
    ROW_NUMBER() OVER (
      PARTITION BY source_table, business_key, change_timestamp
      ORDER BY source_table
    ) AS rn
  FROM unioned
)
SELECT
  business_key,
  change_timestamp,
  changed_by
FROM dedup
WHERE rn = 1
"""
audit_trail_silver_df = spark.sql(audit_trail_silver_sql)
audit_trail_silver_df.createOrReplaceTempView("audit_trail_silver")

(
    audit_trail_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/audit_trail_silver.csv")
)

# --------------------------------------------------------------------
# Target: data_lineage_silver
# --------------------------------------------------------------------
data_lineage_silver_sql = """
WITH src AS (
  SELECT DISTINCT
    peb.source_system AS source_system
  FROM patient_enrollment_bronze peb
)
SELECT
  source_system
FROM src
"""
data_lineage_silver_df = spark.sql(data_lineage_silver_sql)
data_lineage_silver_df.createOrReplaceTempView("data_lineage_silver")

(
    data_lineage_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_lineage_silver.csv")
)

# --------------------------------------------------------------------
# Target: historical_versions_silver
# --------------------------------------------------------------------
historical_versions_silver_sql = """
SELECT
  cts.trial_id AS record_id,
  cts.start_date AS version_start_date,
  cts.source_system AS source_system
FROM clinical_trials_silver cts
"""
historical_versions_silver_df = spark.sql(historical_versions_silver_sql)
historical_versions_silver_df.createOrReplaceTempView("historical_versions_silver")

(
    historical_versions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/historical_versions_silver.csv")
)

job.commit()
