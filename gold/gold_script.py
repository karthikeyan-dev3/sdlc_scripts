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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) READ SOURCE TABLES FROM S3
# ------------------------------------------------------------------------------

clinical_trial_site_enrollment_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_site_enrollment_silver.{FILE_FORMAT}/")
)

clinical_trial_site_visit_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_site_visit_silver.{FILE_FORMAT}/")
)

clinical_trial_metadata_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trial_metadata_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) CREATE TEMP VIEWS
# ------------------------------------------------------------------------------

clinical_trial_site_enrollment_silver_df.createOrReplaceTempView("clinical_trial_site_enrollment_silver")
clinical_trial_site_visit_silver_df.createOrReplaceTempView("clinical_trial_site_visit_silver")
clinical_trial_metadata_silver_df.createOrReplaceTempView("clinical_trial_metadata_silver")

# ------------------------------------------------------------------------------
# 3) TRANSFORMATIONS USING SPARK SQL
# ------------------------------------------------------------------------------

# --- Target: gold_clinical_trial_kpis ---
gold_clinical_trial_kpis_df = spark.sql(
    """
    WITH base AS (
        SELECT
            ctses.trial_id AS trial_id,
            ctses.country AS country,
            ctses.site_id AS site_id,

            COUNT(DISTINCT ctses.patient_id) OVER (
                PARTITION BY ctses.trial_id, ctses.country, ctses.site_id
            ) AS total_enrolled_patients,

            (
                COUNT(DISTINCT ctsvs.visit_id) OVER (
                    PARTITION BY ctses.trial_id, ctses.country, ctses.site_id
                ) /
                NULLIF(
                    COUNT(DISTINCT ctses.patient_id) OVER (
                        PARTITION BY ctses.trial_id, ctses.country, ctses.site_id
                    ),
                    0
                )
            ) * 100 AS visit_completion_percentage,

            CASE
                WHEN COUNT(DISTINCT CASE WHEN CAST(ctses.enrollment_date AS DATE) >= CURRENT_DATE - 30 THEN ctses.patient_id END) OVER (
                    PARTITION BY ctses.trial_id, ctses.country, ctses.site_id
                ) = 0
                THEN 'delayed'
                ELSE 'on track'
            END AS enrollment_status,

            CURRENT_TIMESTAMP AS date_aggregated
        FROM clinical_trial_site_enrollment_silver ctses
        LEFT JOIN clinical_trial_site_visit_silver ctsvs
            ON ctses.trial_id = ctsvs.trial_id
           AND ctses.country = ctsvs.country
           AND ctses.site_id = ctsvs.site_id
    )
    SELECT DISTINCT
        trial_id,
        country,
        site_id,
        CAST(total_enrolled_patients AS INT) AS total_enrolled_patients,
        CAST(visit_completion_percentage AS DECIMAL(38, 10)) AS visit_completion_percentage,
        enrollment_status,
        date_aggregated
    FROM base
    """
)

# --- Target: gold_clinical_trial_trends ---
gold_clinical_trial_trends_df = spark.sql(
    """
    WITH base AS (
        SELECT
            ctses.trial_id AS trial_id,
            COALESCE(CAST(ctses.enrollment_date AS DATE), CAST(ctsvs.visit_date AS DATE)) AS date,

            COUNT(DISTINCT ctses.patient_id) OVER (
                PARTITION BY ctses.trial_id, CAST(ctses.enrollment_date AS DATE)
            ) AS enrollment_trend,

            (
                COUNT(DISTINCT ctsvs.visit_id) OVER (
                    PARTITION BY ctses.trial_id, COALESCE(CAST(ctses.enrollment_date AS DATE), CAST(ctsvs.visit_date AS DATE))
                ) /
                NULLIF(
                    COUNT(DISTINCT ctses.patient_id) OVER (
                        PARTITION BY ctses.trial_id, CAST(ctses.enrollment_date AS DATE)
                    ),
                    0
                )
            ) * 100 AS visit_completion_trend
        FROM clinical_trial_site_enrollment_silver ctses
        LEFT JOIN clinical_trial_site_visit_silver ctsvs
            ON ctses.trial_id = ctsvs.trial_id
           AND CAST(ctses.enrollment_date AS DATE) = CAST(ctsvs.visit_date AS DATE)
    )
    SELECT DISTINCT
        trial_id,
        date,
        CAST(enrollment_trend AS INT) AS enrollment_trend,
        CAST(visit_completion_trend AS DECIMAL(38, 10)) AS visit_completion_trend
    FROM base
    """
)

# --- Target: gold_clinical_trial_metadata ---
gold_clinical_trial_metadata_df = spark.sql(
    """
    SELECT
        ctms.trial_id AS trial_id,
        ctms.start_date AS start_date
    FROM clinical_trial_metadata_silver ctms
    """
)

# ------------------------------------------------------------------------------
# 4) WRITE EACH TARGET TABLE SEPARATELY (SINGLE CSV FILE DIRECTLY UNDER TARGET_PATH)
# ------------------------------------------------------------------------------

(
    gold_clinical_trial_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_kpis.csv")
)

(
    gold_clinical_trial_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_trends.csv")
)

(
    gold_clinical_trial_metadata_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_metadata.csv")
)

job.commit()