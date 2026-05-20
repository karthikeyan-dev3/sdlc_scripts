import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
patient_enrollment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)

clinical_visit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
patient_enrollment_bronze_df.createOrReplaceTempView("patient_enrollment_bronze")
clinical_visit_bronze_df.createOrReplaceTempView("clinical_visit_bronze")

# ============================================================
# Target: clinical_trial_metadata_silver (ctms)
# ============================================================
ctms_df = spark.sql(
    """
    WITH base AS (
      SELECT
        peb.trial_id,
        CAST(peb.enrollment_date AS DATE) AS enrollment_dt
      FROM patient_enrollment_bronze peb
    ),
    agg AS (
      SELECT
        trial_id,
        MIN(enrollment_dt) AS start_date
      FROM base
      GROUP BY trial_id
    ),
    dedup AS (
      SELECT
        trial_id,
        start_date,
        ROW_NUMBER() OVER (
          PARTITION BY trial_id
          ORDER BY start_date DESC
        ) AS rn
      FROM agg
    )
    SELECT
      trial_id,
      start_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    ctms_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_metadata_silver.csv")
)

# ============================================================
# Target: clinical_trial_site_enrollment_silver (ctses)
# ============================================================
ctses_df = spark.sql(
    """
    WITH filtered AS (
      SELECT
        peb.trial_id,
        UPPER(TRIM(peb.country)) AS country,
        peb.site_id,
        peb.patient_id,
        peb.enrollment_date
      FROM patient_enrollment_bronze peb
      WHERE UPPER(TRIM(peb.consent_status)) IN ('CONSENTED','YES','Y','TRUE','1')
    ),
    ranked AS (
      SELECT
        trial_id,
        country,
        site_id,
        patient_id,
        enrollment_date,
        ROW_NUMBER() OVER (
          PARTITION BY trial_id, site_id, patient_id
          ORDER BY enrollment_date DESC
        ) AS rn
      FROM filtered
    )
    SELECT
      trial_id,
      country,
      site_id,
      patient_id,
      enrollment_date
    FROM ranked
    WHERE rn = 1
    """
)

(
    ctses_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_site_enrollment_silver.csv")
)

# ============================================================
# Target: clinical_trial_site_visit_silver (ctsvs)
# ============================================================
ctsvs_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        cvb.trial_id,
        UPPER(TRIM(peb.country)) AS country,
        peb.site_id,
        cvb.patient_id,
        cvb.visit_id,
        cvb.visit_date,
        cvb.visit_type
      FROM clinical_visit_bronze cvb
      INNER JOIN patient_enrollment_bronze peb
        ON cvb.trial_id = peb.trial_id
       AND cvb.patient_id = peb.patient_id
      WHERE cvb.visit_date IS NOT NULL
    ),
    ranked AS (
      SELECT
        trial_id,
        country,
        site_id,
        patient_id,
        visit_id,
        visit_date,
        visit_type,
        ROW_NUMBER() OVER (
          PARTITION BY trial_id, patient_id, visit_id
          ORDER BY visit_date DESC
        ) AS rn
      FROM joined
    )
    SELECT
      trial_id,
      country,
      site_id,
      patient_id,
      visit_id,
      visit_date,
      visit_type
    FROM ranked
    WHERE rn = 1
    """
)

(
    ctsvs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_site_visit_silver.csv")
)
