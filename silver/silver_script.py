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

# --------------------------------------------------------------------------------------
# Source Reads (Bronze)
# --------------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------
# Target: silver.patient_enrollment_silver
# --------------------------------------------------------------------------------------
patient_enrollment_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(peb.site_id) AS string)          AS site_id,
            CAST(TRIM(peb.trial_id) AS string)         AS trial_id,
            CAST(TRIM(peb.country) AS string)          AS country,
            CAST(TRIM(peb.patient_id) AS string)       AS patient_id,
            CAST(peb.enrollment_date AS timestamp)     AS enrollment_date,
            CAST(TRIM(peb.consent_status) AS string)   AS consent_status
        FROM patient_enrollment_bronze peb
        WHERE TRIM(peb.trial_id) IS NOT NULL AND TRIM(peb.trial_id) <> ''
          AND TRIM(peb.site_id) IS NOT NULL AND TRIM(peb.site_id) <> ''
          AND TRIM(peb.patient_id) IS NOT NULL AND TRIM(peb.patient_id) <> ''
    ),
    dedup AS (
        SELECT
            site_id,
            trial_id,
            country,
            patient_id,
            enrollment_date,
            consent_status,
            ROW_NUMBER() OVER (
                PARTITION BY trial_id, site_id, patient_id
                ORDER BY enrollment_date DESC
            ) AS rn
        FROM base
    )
    SELECT
        site_id,
        trial_id,
        country,
        patient_id,
        enrollment_date,
        consent_status
    FROM dedup
    WHERE rn = 1
    """
)
patient_enrollment_silver_df.createOrReplaceTempView("patient_enrollment_silver")

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/patient_enrollment_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.clinical_visit_silver
# --------------------------------------------------------------------------------------
clinical_visit_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(cvb.visit_id) AS string)       AS visit_id,
            CAST(TRIM(cvb.patient_id) AS string)     AS patient_id,
            CAST(TRIM(cvb.trial_id) AS string)       AS trial_id,
            CAST(cvb.visit_date AS timestamp)        AS visit_date,
            CAST(TRIM(cvb.visit_type) AS string)     AS visit_type
        FROM clinical_visit_bronze cvb
        WHERE TRIM(cvb.trial_id) IS NOT NULL AND TRIM(cvb.trial_id) <> ''
          AND TRIM(cvb.patient_id) IS NOT NULL AND TRIM(cvb.patient_id) <> ''
          AND TRIM(cvb.visit_id) IS NOT NULL AND TRIM(cvb.visit_id) <> ''
    ),
    dedup AS (
        SELECT
            visit_id,
            patient_id,
            trial_id,
            visit_date,
            visit_type,
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
        visit_type
    FROM dedup
    WHERE rn = 1
    """
)
clinical_visit_silver_df.createOrReplaceTempView("clinical_visit_silver")

(
    clinical_visit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/clinical_visit_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.trial_site_patient_status_silver
# --------------------------------------------------------------------------------------
trial_site_patient_status_silver_df = spark.sql(
    """
    SELECT
        CAST(TRIM(pes.trial_id) AS string)        AS trial_id,
        CAST(TRIM(pes.site_id) AS string)         AS site_id,
        CAST(TRIM(pes.country) AS string)         AS country,
        CAST(TRIM(pes.patient_id) AS string)      AS patient_id,
        CAST(pes.enrollment_date AS timestamp)    AS enrollment_date,
        CAST(TRIM(pes.consent_status) AS string)  AS consent_status
    FROM patient_enrollment_silver pes
    LEFT JOIN clinical_visit_silver cvs
        ON pes.trial_id = cvs.trial_id
       AND pes.patient_id = cvs.patient_id
    """
)
trial_site_patient_status_silver_df.createOrReplaceTempView("trial_site_patient_status_silver")

(
    trial_site_patient_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_site_patient_status_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.trial_site_daily_metrics_silver
# --------------------------------------------------------------------------------------
trial_site_daily_metrics_silver_df = spark.sql(
    """
    SELECT
        CAST(TRIM(tsps.trial_id) AS string) AS trial_id,
        CAST(TRIM(tsps.country) AS string)  AS country,
        CAST(TRIM(tsps.site_id) AS string)  AS site_id,
        CAST(COALESCE(cvs.visit_date, tsps.enrollment_date) AS date) AS metric_date
    FROM trial_site_patient_status_silver tsps
    LEFT JOIN clinical_visit_silver cvs
        ON tsps.trial_id = cvs.trial_id
       AND tsps.patient_id = cvs.patient_id
    GROUP BY
        tsps.trial_id,
        tsps.country,
        tsps.site_id,
        CAST(COALESCE(cvs.visit_date, tsps.enrollment_date) AS date)
    """
)
trial_site_daily_metrics_silver_df.createOrReplaceTempView("trial_site_daily_metrics_silver")

(
    trial_site_daily_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_site_daily_metrics_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.site_alerts_silver
# --------------------------------------------------------------------------------------
site_alerts_silver_df = spark.sql(
    """
    SELECT
        CAST(TRIM(tsdm.trial_id) AS string)   AS trial_id,
        CAST(TRIM(tsdm.country) AS string)   AS country,
        CAST(TRIM(tsdm.site_id) AS string)   AS site_id,
        CAST(tsdm.metric_date AS date)       AS metric_date
    FROM trial_site_daily_metrics_silver tsdm
    """
)
site_alerts_silver_df.createOrReplaceTempView("site_alerts_silver")

(
    site_alerts_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/site_alerts_silver.csv")
)

job.commit()