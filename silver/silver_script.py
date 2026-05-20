import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
peb_df.createOrReplaceTempView("patient_enrollment_bronze")

cvb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
)
cvb_df.createOrReplaceTempView("clinical_visit_bronze")

# ------------------------------------------------------------------------------------
# Target: clinical_trial_patient_status_silver (ctps)
# Source: bronze.patient_enrollment_bronze (peb) with dedup via ROW_NUMBER
# ------------------------------------------------------------------------------------
ctps_sql = """
WITH ranked AS (
  SELECT
    peb.trial_id AS trial_id,
    peb.site_id AS site_id,
    peb.patient_id AS patient_id,
    UPPER(TRIM(peb.country)) AS country,
    CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
    UPPER(TRIM(peb.consent_status)) AS consent_status,
    CASE
      WHEN UPPER(TRIM(peb.consent_status)) IN ('WITHDRAWN','DROPPED','DROPOUT') THEN 'DROPPED_OUT'
      WHEN UPPER(TRIM(peb.consent_status)) IN ('COMPLETED','COMPLETE') THEN 'COMPLETED'
      WHEN UPPER(TRIM(peb.consent_status)) IN ('CONSENTED','ENROLLED','ACTIVE') THEN 'ACTIVE'
      ELSE 'UNKNOWN'
    END AS enrollment_status,
    ROW_NUMBER() OVER (
      PARTITION BY peb.trial_id, peb.site_id, peb.patient_id
      ORDER BY peb.enrollment_date DESC, peb.source_system DESC
    ) AS rn
  FROM patient_enrollment_bronze peb
)
SELECT
  trial_id,
  site_id,
  patient_id,
  country,
  enrollment_date,
  consent_status,
  enrollment_status
FROM ranked
WHERE rn = 1
"""
ctps_df = spark.sql(ctps_sql)
ctps_df.createOrReplaceTempView("clinical_trial_patient_status_silver")

(
    ctps_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_patient_status_silver.csv")
)

# ------------------------------------------------------------------------------------
# Target: patient_visit_tracking_silver (pvts)
# Source: bronze.clinical_visit_bronze (cvb) INNER JOIN silver.ctps with dedup
# ------------------------------------------------------------------------------------
pvts_sql = """
WITH joined AS (
  SELECT
    cvb.trial_id AS trial_id,
    cvb.patient_id AS patient_id,
    ctps.site_id AS site_id,
    cvb.visit_id AS visit_id,
    CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
    UPPER(TRIM(cvb.visit_type)) AS visit_type,
    CASE
      WHEN cvb.visit_date IS NOT NULL THEN 'COMPLETED'
      ELSE 'UNKNOWN'
    END AS visit_status,
    ROW_NUMBER() OVER (
      PARTITION BY cvb.trial_id, cvb.patient_id, cvb.visit_id
      ORDER BY cvb.visit_date DESC, cvb.source_system DESC
    ) AS rn
  FROM clinical_visit_bronze cvb
  INNER JOIN clinical_trial_patient_status_silver ctps
    ON cvb.trial_id = ctps.trial_id
   AND cvb.patient_id = ctps.patient_id
)
SELECT
  trial_id,
  patient_id,
  site_id,
  visit_id,
  visit_date,
  visit_type,
  visit_status
FROM joined
WHERE rn = 1
"""
pvts_df = spark.sql(pvts_sql)
pvts_df.createOrReplaceTempView("patient_visit_tracking_silver")

(
    pvts_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_visit_tracking_silver.csv")
)

# ------------------------------------------------------------------------------------
# Target: clinical_trial_kpi_daily_silver (ctkds)
# Source: silver.ctps LEFT JOIN silver.pvts GROUP BY trial,country,site,date
# ------------------------------------------------------------------------------------
ctkds_sql = """
SELECT
  ctps.trial_id AS trial_id,
  ctps.country AS country,
  ctps.site_id AS site_id,
  CAST(COALESCE(pvts.visit_date, ctps.enrollment_date) AS DATE) AS date,
  COUNT(DISTINCT ctps.patient_id) AS total_enrolled_patients,
  COUNT(DISTINCT CASE WHEN ctps.enrollment_status = 'ACTIVE' THEN ctps.patient_id END) AS active_patients,
  COUNT(DISTINCT CASE WHEN ctps.enrollment_status = 'DROPPED_OUT' THEN ctps.patient_id END) AS patient_dropout_count,
  COUNT(DISTINCT CASE WHEN pvts.visit_status = 'COMPLETED' THEN pvts.visit_id END) AS completed_visit_count,
  CASE
    WHEN COUNT(DISTINCT pvts.visit_id) = 0 THEN NULL
    ELSE (COUNT(DISTINCT CASE WHEN pvts.visit_status = 'COMPLETED' THEN pvts.visit_id END) * 100.0)
         / COUNT(DISTINCT pvts.visit_id)
  END AS visit_completion_percentage
FROM clinical_trial_patient_status_silver ctps
LEFT JOIN patient_visit_tracking_silver pvts
  ON ctps.trial_id = pvts.trial_id
 AND ctps.site_id = pvts.site_id
 AND ctps.patient_id = pvts.patient_id
GROUP BY
  ctps.trial_id,
  ctps.country,
  ctps.site_id,
  CAST(COALESCE(pvts.visit_date, ctps.enrollment_date) AS DATE)
"""
ctkds_df = spark.sql(ctkds_sql)
ctkds_df.createOrReplaceTempView("clinical_trial_kpi_daily_silver")

(
    ctkds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_kpi_daily_silver.csv")
)

# ------------------------------------------------------------------------------------
# Target: site_performance_metrics_daily_silver (spmds)
# Source: silver.ctkds GROUP BY site_id,date
# ------------------------------------------------------------------------------------
spmds_sql = """
SELECT
  ctkds.site_id AS site_id,
  ctkds.date AS date,
  CASE
    WHEN SUM(ctkds.total_enrolled_patients) = 0 THEN NULL
    ELSE ((SUM(ctkds.total_enrolled_patients) - SUM(ctkds.patient_dropout_count)) * 1.0)
         / SUM(ctkds.total_enrolled_patients)
  END AS patient_retention_rate,
  AVG(ctkds.visit_completion_percentage) AS visit_adherence_rate
FROM clinical_trial_kpi_daily_silver ctkds
GROUP BY
  ctkds.site_id,
  ctkds.date
"""
spmds_df = spark.sql(spmds_sql)
spmds_df.createOrReplaceTempView("site_performance_metrics_daily_silver")

(
    spmds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_performance_metrics_daily_silver.csv")
)

# ------------------------------------------------------------------------------------
# Target: historical_kpi_records_silver (hkrs)
# Source: silver.ctkds GROUP BY trial_id,date
# ------------------------------------------------------------------------------------
hkrs_sql = """
SELECT
  ctkds.trial_id AS trial_id,
  ctkds.date AS date,
  SUM(ctkds.total_enrolled_patients) AS total_enrolled_patients,
  SUM(ctkds.patient_dropout_count) AS patient_dropout_count,
  AVG(ctkds.visit_completion_percentage) AS visit_completion_percentage,
  CASE
    WHEN (
      SUM(CASE WHEN ctkds.total_enrolled_patients IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN ctkds.visit_completion_percentage IS NULL THEN 1 ELSE 0 END)
    ) = 0 THEN 100
    ELSE
      100
      - 50 * (SUM(CASE WHEN ctkds.total_enrolled_patients IS NULL THEN 1 ELSE 0 END) > 0)
      - 50 * (SUM(CASE WHEN ctkds.visit_completion_percentage IS NULL THEN 1 ELSE 0 END) > 0)
  END AS data_quality_score
FROM clinical_trial_kpi_daily_silver ctkds
GROUP BY
  ctkds.trial_id,
  ctkds.date
"""
hkrs_df = spark.sql(hkrs_sql)
hkrs_df.createOrReplaceTempView("historical_kpi_records_silver")

(
    hkrs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/historical_kpi_records_silver.csv")
)
