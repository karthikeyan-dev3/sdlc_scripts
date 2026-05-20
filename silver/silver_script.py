import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Source Reads + Temp Views
# -----------------------------
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

# -----------------------------
# Target: trial_site_country_patient_silver
# -----------------------------
trial_site_country_patient_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(peb.patient_id AS STRING) AS patient_id,
            CAST(peb.trial_id AS STRING) AS trial_id,
            CAST(peb.site_id AS STRING) AS site_id,
            UPPER(TRIM(CAST(peb.country AS STRING))) AS country,
            CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
            CAST(peb.consent_status AS STRING) AS consent_status,
            CAST(peb.source_system AS STRING) AS source_system,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(peb.patient_id AS STRING), CAST(peb.trial_id AS STRING)
                ORDER BY CAST(peb.enrollment_date AS TIMESTAMP) DESC
            ) AS rn
        FROM patient_enrollment_bronze peb
        WHERE UPPER(TRIM(CAST(peb.consent_status AS STRING))) IN ('CONSENTED', 'ENROLLED', 'YES', 'Y', 'TRUE')
    )
    SELECT
        patient_id,
        trial_id,
        site_id,
        country,
        enrollment_date,
        consent_status,
        source_system
    FROM base
    WHERE rn = 1
    """
)
trial_site_country_patient_silver_df.createOrReplaceTempView("trial_site_country_patient_silver")

(
    trial_site_country_patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_country_patient_silver.csv")
)

# -----------------------------
# Target: trial_site_patient_visit_silver
# -----------------------------
trial_site_patient_visit_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(cvb.visit_id AS STRING) AS visit_id,
            CAST(cvb.patient_id AS STRING) AS patient_id,
            CAST(cvb.trial_id AS STRING) AS trial_id,
            CAST(tscps.site_id AS STRING) AS site_id,
            UPPER(TRIM(CAST(tscps.country AS STRING))) AS country,
            CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
            UPPER(TRIM(CAST(cvb.visit_type AS STRING))) AS visit_type,
            CAST(cvb.source_system AS STRING) AS source_system,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(cvb.visit_id AS STRING)
                ORDER BY CAST(cvb.visit_date AS TIMESTAMP) DESC
            ) AS rn
        FROM clinical_visit_bronze cvb
        INNER JOIN trial_site_country_patient_silver tscps
            ON CAST(cvb.patient_id AS STRING) = CAST(tscps.patient_id AS STRING)
            AND CAST(cvb.trial_id AS STRING) = CAST(tscps.trial_id AS STRING)
        WHERE cvb.patient_id IS NOT NULL
          AND cvb.trial_id IS NOT NULL
          AND cvb.visit_date IS NOT NULL
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
    trial_site_patient_visit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_patient_visit_silver.csv")
)

job.commit()