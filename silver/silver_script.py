import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables from S3
# ----------------------------
patient_enrollments_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollments_bronze.{FILE_FORMAT}/")
)

visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/visits_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
patient_enrollments_bronze_df.createOrReplaceTempView("patient_enrollments_bronze")
visits_bronze_df.createOrReplaceTempView("visits_bronze")

# ============================================================
# TARGET TABLE: trials_silver
# ============================================================
trials_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        peb.trial_id AS trial_id,
        CONCAT('Trial ', peb.trial_id) AS trial_name,
        ROW_NUMBER() OVER (
          PARTITION BY peb.trial_id
          ORDER BY peb.trial_id
        ) AS rn
      FROM patient_enrollments_bronze peb
    )
    SELECT
      trial_id,
      trial_name
    FROM base
    WHERE rn = 1
    """
)

(
    trials_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trials_silver.csv")
)

# ============================================================
# TARGET TABLE: sites_silver
# ============================================================
sites_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        peb.site_id AS site_id,
        peb.trial_id AS trial_id,
        COALESCE(NULLIF(TRIM(UPPER(peb.country)), ''), 'UNKNOWN') AS country,
        ROW_NUMBER() OVER (
          PARTITION BY peb.site_id, peb.trial_id
          ORDER BY COALESCE(NULLIF(TRIM(UPPER(peb.country)), ''), 'UNKNOWN') DESC
        ) AS rn
      FROM patient_enrollments_bronze peb
    )
    SELECT
      site_id,
      trial_id,
      country
    FROM base
    WHERE rn = 1
    """
)

(
    sites_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sites_silver.csv")
)

# ============================================================
# TARGET TABLE: patient_enrollments_silver
# ============================================================
patient_enrollments_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        peb.patient_id AS patient_id,
        peb.trial_id AS trial_id,
        peb.site_id AS site_id,
        COALESCE(NULLIF(TRIM(UPPER(peb.country)), ''), 'UNKNOWN') AS country,
        peb.enrollment_date AS enrollment_date,
        peb.consent_status AS consent_status,
        CASE
          WHEN UPPER(peb.consent_status) IN ('WITHDRAWN','DROPPED','DROP_OUT') THEN 'DROPPED'
          WHEN UPPER(peb.consent_status) IN ('ACTIVE','CONSENTED') THEN 'ACTIVE'
          ELSE 'UNKNOWN'
        END AS patient_status,
        peb.source_system AS source_system,
        ROW_NUMBER() OVER (
          PARTITION BY peb.patient_id, peb.trial_id
          ORDER BY peb.enrollment_date DESC
        ) AS rn
      FROM patient_enrollments_bronze peb
    )
    SELECT
      patient_id,
      trial_id,
      site_id,
      country,
      enrollment_date,
      consent_status,
      patient_status,
      source_system
    FROM base
    WHERE rn = 1
    """
)

patient_enrollments_silver_df.createOrReplaceTempView("patient_enrollments_silver")

(
    patient_enrollments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollments_silver.csv")
)

# ============================================================
# TARGET TABLE: patient_visits_silver
# ============================================================
patient_visits_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        vb.visit_id AS visit_id,
        vb.patient_id AS patient_id,
        vb.trial_id AS trial_id,
        pes.site_id AS site_id,
        pes.country AS country,
        vb.visit_date AS visit_date,
        vb.visit_type AS visit_type,
        vb.source_system AS source_system,
        ROW_NUMBER() OVER (
          PARTITION BY vb.visit_id
          ORDER BY vb.visit_id
        ) AS rn
      FROM visits_bronze vb
      INNER JOIN patient_enrollments_silver pes
        ON vb.patient_id = pes.patient_id
       AND vb.trial_id = pes.trial_id
      WHERE vb.visit_date IS NOT NULL
    )
    SELECT
      visit_id,
      patient_id,
      trial_id,
      site_id,
      country,
      visit_date,
      visit_type,
      source_system
    FROM base
    WHERE rn = 1
    """
)

(
    patient_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_visits_silver.csv")
)

job.commit()