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

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
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

aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
aeb_df.createOrReplaceTempView("adverse_events_bronze")

# ============================================================
# 1) Target: clinical_trial_site_dim_silver (ctsd)
# ============================================================
clinical_trial_site_dim_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(peb.trial_id AS STRING) AS trial_id,
    UPPER(TRIM(CAST(peb.country AS STRING))) AS country,
    CAST(peb.site_id AS STRING) AS site_id,
    CAST(NULL AS STRING) AS trial_name,
    CAST(NULL AS STRING) AS site_name
  FROM patient_enrollment_bronze peb
  LEFT JOIN clinical_visit_bronze cvb
    ON peb.trial_id = cvb.trial_id
   AND peb.site_id = cvb.site_id
),
dedup AS (
  SELECT
    trial_id,
    trial_name,
    country,
    site_id,
    site_name,
    ROW_NUMBER() OVER (
      PARTITION BY trial_id, country, site_id
      ORDER BY trial_id, country, site_id
    ) AS rn
  FROM base
)
SELECT
  trial_id,
  trial_name,
  country,
  site_id,
  site_name
FROM dedup
WHERE rn = 1
""")

(
    clinical_trial_site_dim_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_site_dim_silver.csv")
)

clinical_trial_site_dim_silver_df.createOrReplaceTempView("clinical_trial_site_dim_silver")

# ============================================================
# 2) Target: patient_enrollment_clean_silver (pec)
# ============================================================
patient_enrollment_clean_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(peb.patient_id AS STRING) AS patient_id,
    CAST(peb.trial_id AS STRING) AS trial_id,
    CAST(peb.site_id AS STRING) AS site_id,
    UPPER(TRIM(CAST(peb.country AS STRING))) AS country,
    CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
    UPPER(TRIM(CAST(peb.consent_status AS STRING))) AS consent_status,
    CAST(peb.source_system AS STRING) AS source_system
  FROM patient_enrollment_bronze peb
  WHERE peb.patient_id IS NOT NULL
),
ranked AS (
  SELECT
    patient_id,
    trial_id,
    site_id,
    country,
    enrollment_date,
    consent_status,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, trial_id, site_id
      ORDER BY
        enrollment_date DESC,
        CASE WHEN consent_status = 'CONSENTED' THEN 1 ELSE 2 END ASC
    ) AS rn
  FROM base
)
SELECT
  patient_id,
  trial_id,
  site_id,
  country,
  enrollment_date,
  consent_status,
  source_system
FROM ranked
WHERE rn = 1
""")

(
    patient_enrollment_clean_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollment_clean_silver.csv")
)

patient_enrollment_clean_silver_df.createOrReplaceTempView("patient_enrollment_clean_silver")

# ============================================================
# 3) Target: clinical_visit_clean_silver (cvc)
# ============================================================
clinical_visit_clean_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(cvb.visit_id AS STRING) AS visit_id,
    CAST(cvb.patient_id AS STRING) AS patient_id,
    CAST(cvb.trial_id AS STRING) AS trial_id,
    CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
    TRIM(CAST(cvb.visit_type AS STRING)) AS visit_type,
    CAST(cvb.source_system AS STRING) AS source_system
  FROM clinical_visit_bronze cvb
  WHERE cvb.visit_id IS NOT NULL
),
dedup AS (
  SELECT
    visit_id,
    patient_id,
    trial_id,
    visit_date,
    visit_type,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY visit_id
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
  source_system
FROM dedup
WHERE rn = 1
""")

(
    clinical_visit_clean_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_visit_clean_silver.csv")
)

clinical_visit_clean_silver_df.createOrReplaceTempView("clinical_visit_clean_silver")

# ============================================================
# 4) Target: patient_status_silver (ps)
# ============================================================
patient_status_silver_df = spark.sql("""
SELECT
  CAST(pec.patient_id AS STRING) AS patient_id,
  CAST(pec.trial_id AS STRING) AS trial_id,
  CAST(pec.site_id AS STRING) AS site_id,
  CAST(pec.country AS STRING) AS country,
  CAST(pec.enrollment_date AS TIMESTAMP) AS enrollment_date,
  CAST(
    CASE
      WHEN UPPER(TRIM(CAST(pec.consent_status AS STRING))) IN ('WITHDRAWN','DROPOUT') THEN 1
      ELSE 0
    END
    AS BOOLEAN
  ) AS dropout_flag,
  CAST(aeb.event_end_date AS TIMESTAMP) AS dropout_date
FROM patient_enrollment_clean_silver pec
LEFT JOIN adverse_events_bronze aeb
  ON pec.patient_id = aeb.patient_id
""")

(
    patient_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_status_silver.csv")
)

patient_status_silver_df.createOrReplaceTempView("patient_status_silver")

# ============================================================
# 5) Target: site_trial_kpi_daily_silver (stk)
# ============================================================
site_trial_kpi_daily_silver_df = spark.sql("""
WITH enrollment_daily AS (
  SELECT
    CAST(ps.trial_id AS STRING) AS trial_id,
    CAST(ps.country AS STRING) AS country,
    CAST(ps.site_id AS STRING) AS site_id,
    DATE(CAST(ps.enrollment_date AS TIMESTAMP)) AS kpi_date,
    COUNT(DISTINCT ps.patient_id) AS total_enrolled_patients,
    (COUNT(DISTINCT ps.patient_id) - COUNT(DISTINCT CASE WHEN ps.dropout_flag = TRUE THEN ps.patient_id END)) AS active_patients,
    COUNT(DISTINCT CASE WHEN ps.dropout_flag = TRUE THEN ps.patient_id END) AS patient_dropout_count
  FROM patient_status_silver ps
  GROUP BY
    CAST(ps.trial_id AS STRING),
    CAST(ps.country AS STRING),
    CAST(ps.site_id AS STRING),
    DATE(CAST(ps.enrollment_date AS TIMESTAMP))
),
visit_daily AS (
  SELECT
    CAST(ps.trial_id AS STRING) AS trial_id,
    CAST(ps.country AS STRING) AS country,
    CAST(ps.site_id AS STRING) AS site_id,
    DATE(CAST(cvc.visit_date AS TIMESTAMP)) AS kpi_date,
    COUNT(DISTINCT cvc.visit_id) AS completed_visits
  FROM patient_status_silver ps
  LEFT JOIN clinical_visit_clean_silver cvc
    ON ps.patient_id = cvc.patient_id
   AND ps.trial_id = cvc.trial_id
  GROUP BY
    CAST(ps.trial_id AS STRING),
    CAST(ps.country AS STRING),
    CAST(ps.site_id AS STRING),
    DATE(CAST(cvc.visit_date AS TIMESTAMP))
),
combined AS (
  SELECT
    COALESCE(e.trial_id, v.trial_id) AS trial_id,
    COALESCE(e.country, v.country) AS country,
    COALESCE(e.site_id, v.site_id) AS site_id,
    COALESCE(e.kpi_date, v.kpi_date) AS kpi_date,
    COALESCE(e.total_enrolled_patients, 0) AS total_enrolled_patients,
    COALESCE(e.active_patients, 0) AS active_patients,
    COALESCE(e.patient_dropout_count, 0) AS patient_dropout_count,
    v.completed_visits AS completed_visits
  FROM enrollment_daily e
  FULL OUTER JOIN visit_daily v
    ON e.trial_id = v.trial_id
   AND e.country = v.country
   AND e.site_id = v.site_id
   AND e.kpi_date = v.kpi_date
)
SELECT
  trial_id,
  country,
  site_id,
  kpi_date,
  CAST(total_enrolled_patients AS BIGINT) AS total_enrolled_patients,
  CAST(active_patients AS BIGINT) AS active_patients,
  CAST(patient_dropout_count AS BIGINT) AS patient_dropout_count,
  CAST(
    CASE
      WHEN completed_visits IS NULL THEN NULL
      ELSE 1.0
    END AS DOUBLE
  ) AS visit_completion_percentage,
  CAST(
    CASE
      WHEN total_enrolled_patients = 0 THEN NULL
      ELSE (CAST(active_patients AS DOUBLE) / CAST(total_enrolled_patients AS DOUBLE))
    END AS DOUBLE
  ) AS patient_retention_rate,
  CAST(NULL AS DOUBLE) AS visit_adherence_rate
FROM combined
WHERE kpi_date IS NOT NULL
""")

(
    site_trial_kpi_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_trial_kpi_daily_silver.csv")
)

site_trial_kpi_daily_silver_df.createOrReplaceTempView("site_trial_kpi_daily_silver")

# ============================================================
# 6) Target: site_trial_performance_silver (stp)
# ============================================================
site_trial_performance_silver_df = spark.sql("""
SELECT
  CAST(stk.trial_id AS STRING) AS trial_id,
  CAST(stk.country AS STRING) AS country,
  CAST(stk.site_id AS STRING) AS site_id,
  CAST(stk.kpi_date AS DATE) AS as_of_date,
  CAST(
    CASE
      WHEN stk.patient_retention_rate IS NULL THEN 0
      WHEN stk.patient_retention_rate < 0.8 THEN 1
      ELSE 0
    END AS BOOLEAN
  ) AS underperforming_flag,
  CAST(
    CASE
      WHEN CAST(stk.total_enrolled_patients AS BIGINT) = 0 THEN 1
      ELSE 0
    END AS BOOLEAN
  ) AS enrollment_delay_flag,
  CAST(stk.patient_retention_rate AS DOUBLE) AS patient_retention_rate,
  CAST(stk.visit_adherence_rate AS DOUBLE) AS visit_adherence_rate,
  CAST(stk.total_enrolled_patients AS STRING) AS historical_reports
FROM site_trial_kpi_daily_silver stk
""")

(
    site_trial_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_trial_performance_silver.csv")
)

job.commit()