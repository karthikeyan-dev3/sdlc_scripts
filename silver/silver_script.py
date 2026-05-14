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

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)

pmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_master_bronze.{FILE_FORMAT}/")
)

dmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_master_bronze.{FILE_FORMAT}/")
)

smb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_master_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
aeb_df.createOrReplaceTempView("adverse_events_bronze")
pmb_df.createOrReplaceTempView("patient_master_bronze")
dmb_df.createOrReplaceTempView("drug_master_bronze")
smb_df.createOrReplaceTempView("site_master_bronze")

# -----------------------------------------
# adverse_event_reports_silver (aers)
# -----------------------------------------
aers_df = spark.sql("""
WITH ranked AS (
  SELECT
    aeb.ae_id AS adverse_event_id,
    aeb.patient_id AS patient_id,
    aeb.drug_id AS drug_id,
    aeb.site_id AS site_id,
    DATE(aeb.report_date) AS event_date,
    aeb.severity AS severity,
    aeb.symptom AS description,
    CAST(NULL AS string) AS outcome,
    ROW_NUMBER() OVER (
      PARTITION BY aeb.ae_id
      ORDER BY DATE(aeb.report_date) DESC
    ) AS rn
  FROM adverse_events_bronze aeb
)
SELECT
  adverse_event_id,
  patient_id,
  drug_id,
  site_id,
  event_date,
  severity,
  description,
  outcome
FROM ranked
WHERE rn = 1
""")

(
    aers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_event_reports_silver.csv")
)

# -----------------------------------------
# patient_master_silver (pms)
# -----------------------------------------
pms_df = spark.sql("""
WITH ranked AS (
  SELECT
    pmb.patient_id AS patient_id,
    CAST(pmb.age AS double) AS age,
    UPPER(TRIM(pmb.gender)) AS gender,
    CAST(NULL AS string) AS ethnicity,
    CAST(NULL AS string) AS medical_history,
    ROW_NUMBER() OVER (
      PARTITION BY pmb.patient_id
      ORDER BY DATE(pmb.enrollment_date) DESC
    ) AS rn
  FROM patient_master_bronze pmb
)
SELECT
  patient_id,
  age,
  gender,
  ethnicity,
  medical_history
FROM ranked
WHERE rn = 1
""")

(
    pms_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_master_silver.csv")
)

# -----------------------------------------
# drug_master_silver (dms)
# -----------------------------------------
dms_df = spark.sql("""
WITH ranked AS (
  SELECT
    dmb.drug_id AS drug_id,
    TRIM(dmb.drug_name) AS drug_name,
    CAST(NULL AS string) AS dosage_form,
    CAST(NULL AS date) AS manufacture_date,
    CAST(NULL AS date) AS expiry_date,
    CAST(NULL AS string) AS active_ingredient,
    ROW_NUMBER() OVER (
      PARTITION BY dmb.drug_id
      ORDER BY CASE WHEN dmb.drug_name IS NOT NULL AND TRIM(dmb.drug_name) <> '' THEN 0 ELSE 1 END ASC
    ) AS rn
  FROM drug_master_bronze dmb
)
SELECT
  drug_id,
  drug_name,
  dosage_form,
  manufacture_date,
  expiry_date,
  active_ingredient
FROM ranked
WHERE rn = 1
""")

(
    dms_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_master_silver.csv")
)

# -----------------------------------------
# site_performance_silver (sps)
# -----------------------------------------
sps_df = spark.sql("""
WITH base AS (
  SELECT
    smb.site_id AS site_id,
    CONCAT(smb.city, ', ', smb.country) AS location,
    ROW_NUMBER() OVER (
      PARTITION BY smb.site_id
      ORDER BY smb.site_id
    ) AS rn
  FROM site_master_bronze smb
  LEFT JOIN adverse_events_bronze aeb
    ON smb.site_id = aeb.site_id
)
SELECT
  site_id,
  location,
  CAST(NULL AS string) AS performance_metrics,
  CAST(NULL AS string) AS number_of_active_trials,
  CAST(NULL AS string) AS compliance_rating
FROM base
WHERE rn = 1
""")

(
    sps_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_performance_silver.csv")
)

# -----------------------------------------
# aggregated_trial_data_silver (atds)
# -----------------------------------------
atds_df = spark.sql("""
SELECT
  pmb.trial_phase AS trial_id,
  COUNT(aeb.ae_id) AS total_adverse_events,
  AVG(TRY_CAST(aeb.severity AS double)) AS average_severity,
  COUNT(DISTINCT aeb.patient_id) AS unique_patients,
  COUNT(DISTINCT aeb.drug_id) AS drugs_involved,
  COUNT(DISTINCT aeb.site_id) AS sites_involved
FROM adverse_events_bronze aeb
JOIN patient_master_bronze pmb
  ON aeb.patient_id = pmb.patient_id
GROUP BY
  pmb.trial_phase
""")

(
    atds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_trial_data_silver.csv")
)

job.commit()