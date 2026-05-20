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

# --------------------------------------------------------------------------------------
# 1) Read Source Tables (Bronze)
# --------------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------
# 2) trial_enrollment_patient_silver (teps)
#    - Cleanse + dedup by (patient_id, trial_id) keeping latest enrollment_timestamp
# --------------------------------------------------------------------------------------
trial_enrollment_patient_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        peb.patient_id AS patient_id,
        upper(trim(peb.trial_id)) AS trial_id,
        upper(trim(peb.site_id)) AS site_id,
        upper(trim(peb.country)) AS country,
        cast(peb.enrollment_date as timestamp) AS enrollment_timestamp,
        date(cast(peb.enrollment_date as timestamp)) AS enrolled_date,
        CASE
          WHEN upper(trim(peb.consent_status)) IN ('CONSENTED','NOT_CONSENTED')
            THEN upper(trim(peb.consent_status))
          ELSE 'UNKNOWN'
        END AS consent_status,
        (CASE
          WHEN upper(trim(peb.consent_status)) = 'CONSENTED'
               AND peb.enrollment_date IS NOT NULL
            THEN true
          ELSE false
        END) AS is_enrolled,
        peb.source_system AS source_system
      FROM patient_enrollment_bronze peb
    ),
    dedup AS (
      SELECT
        patient_id,
        trial_id,
        site_id,
        country,
        enrollment_timestamp,
        enrolled_date,
        consent_status,
        is_enrolled,
        source_system,
        row_number() OVER (
          PARTITION BY patient_id, trial_id
          ORDER BY enrollment_timestamp DESC
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      trial_id,
      site_id,
      country,
      enrollment_timestamp,
      enrolled_date,
      consent_status,
      is_enrolled,
      source_system
    FROM dedup
    WHERE rn = 1
    """
)
trial_enrollment_patient_silver_df.createOrReplaceTempView("trial_enrollment_patient_silver")

(
    trial_enrollment_patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_enrollment_patient_silver.csv")
)

# --------------------------------------------------------------------------------------
# 3) trial_visit_patient_silver (tvps)
#    - Cleanse + dedup by visit_id keeping latest visit_timestamp
# --------------------------------------------------------------------------------------
trial_visit_patient_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        cvb.visit_id AS visit_id,
        cvb.patient_id AS patient_id,
        upper(trim(cvb.trial_id)) AS trial_id,
        cast(cvb.visit_date as timestamp) AS visit_timestamp,
        date(cast(cvb.visit_date as timestamp)) AS visit_day,
        cvb.visit_type AS visit_type,
        CAST(cvb.blood_pressure AS double) AS blood_pressure,
        CAST(cvb.heart_rate AS double) AS heart_rate,
        CAST(cvb.weight_kg AS double) AS weight_kg,
        cvb.source_system AS source_system
      FROM clinical_visit_bronze cvb
    ),
    dedup AS (
      SELECT
        visit_id,
        patient_id,
        trial_id,
        visit_timestamp,
        visit_day,
        visit_type,
        blood_pressure,
        heart_rate,
        weight_kg,
        source_system,
        row_number() OVER (
          PARTITION BY visit_id
          ORDER BY visit_timestamp DESC
        ) AS rn
      FROM base
    )
    SELECT
      visit_id,
      patient_id,
      trial_id,
      visit_timestamp,
      visit_day,
      visit_type,
      blood_pressure,
      heart_rate,
      weight_kg,
      source_system
    FROM dedup
    WHERE rn = 1
    """
)
trial_visit_patient_silver_df.createOrReplaceTempView("trial_visit_patient_silver")

(
    trial_visit_patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_visit_patient_silver.csv")
)

# --------------------------------------------------------------------------------------
# 4) trial_patient_status_silver (tpss)
# --------------------------------------------------------------------------------------
trial_patient_status_silver_df = spark.sql(
    """
    SELECT
      teps.patient_id AS patient_id,
      teps.trial_id AS trial_id,
      teps.site_id AS site_id,
      teps.country AS country,
      teps.enrolled_date AS enrolled_date,
      teps.is_enrolled AS is_enrolled,
      max(tvps.visit_timestamp) AS last_visit_timestamp,
      count(distinct tvps.visit_id) AS visit_count,
      (CASE
        WHEN teps.is_enrolled = true AND max(tvps.visit_timestamp) IS NOT NULL THEN true
        ELSE false
      END) AS active_patient_flag,
      (CASE
        WHEN teps.is_enrolled = true AND max(tvps.visit_timestamp) IS NULL THEN true
        ELSE false
      END) AS dropout_flag
    FROM trial_enrollment_patient_silver teps
    LEFT JOIN trial_visit_patient_silver tvps
      ON teps.patient_id = tvps.patient_id
     AND teps.trial_id = tvps.trial_id
    GROUP BY
      teps.patient_id,
      teps.trial_id,
      teps.site_id,
      teps.country,
      teps.enrolled_date,
      teps.is_enrolled
    """
)
trial_patient_status_silver_df.createOrReplaceTempView("trial_patient_status_silver")

(
    trial_patient_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_patient_status_silver.csv")
)

# --------------------------------------------------------------------------------------
# 5) trial_site_country_daily_metrics_silver (tscdm)
# --------------------------------------------------------------------------------------
trial_site_country_daily_metrics_silver_df = spark.sql(
    """
    WITH daily AS (
      SELECT
        teps.trial_id AS trial_id,
        teps.country AS country,
        teps.site_id AS site_id,
        coalesce(date(teps.enrollment_timestamp), tvps.visit_day) AS date,
        count(distinct CASE WHEN teps.is_enrolled THEN teps.patient_id END) AS total_enrolled_patients,
        count(distinct CASE WHEN teps.is_enrolled AND tvps.visit_id IS NOT NULL THEN teps.patient_id END) AS active_patients,
        count(distinct CASE WHEN teps.is_enrolled AND tvps.visit_id IS NULL THEN teps.patient_id END) AS patient_dropout_count,
        100 * (
          count(distinct tvps.visit_id)
          / nullif(count(distinct CASE WHEN teps.is_enrolled THEN teps.patient_id END), 0)
        ) AS visit_completion_percentage
      FROM trial_enrollment_patient_silver teps
      LEFT JOIN trial_visit_patient_silver tvps
        ON teps.trial_id = tvps.trial_id
       AND teps.patient_id = tvps.patient_id
      GROUP BY
        teps.trial_id,
        teps.country,
        teps.site_id,
        coalesce(date(teps.enrollment_timestamp), tvps.visit_day)
    )
    SELECT
      trial_id,
      country,
      site_id,
      date,
      total_enrolled_patients,
      active_patients,
      patient_dropout_count,
      visit_completion_percentage,
      (
        total_enrolled_patients
        - lag(total_enrolled_patients) OVER (PARTITION BY trial_id, country, site_id ORDER BY date)
      ) AS enrollment_trend,
      (
        active_patients
        - lag(active_patients) OVER (PARTITION BY trial_id, country, site_id ORDER BY date)
      ) AS retention_trend
    FROM daily
    """
)
trial_site_country_daily_metrics_silver_df.createOrReplaceTempView("trial_site_country_daily_metrics_silver")

(
    trial_site_country_daily_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_site_country_daily_metrics_silver.csv")
)

# --------------------------------------------------------------------------------------
# 6) trial_data_ingestion_latency_silver (tdils)
# --------------------------------------------------------------------------------------
trial_data_ingestion_latency_silver_df = spark.sql(
    """
    WITH unioned AS (
      SELECT
        upper(trim(peb.trial_id)) AS trial_id,
        cast(peb.enrollment_date as timestamp) AS source_event_timestamp,
        current_timestamp() AS data_ingestion_timestamp
      FROM patient_enrollment_bronze peb

      UNION ALL

      SELECT
        upper(trim(cvb.trial_id)) AS trial_id,
        cast(cvb.visit_date as timestamp) AS source_event_timestamp,
        current_timestamp() AS data_ingestion_timestamp
      FROM clinical_visit_bronze cvb
    )
    SELECT
      trial_id,
      data_ingestion_timestamp,
      (data_ingestion_timestamp - max(source_event_timestamp)) AS metric_update_latency
    FROM unioned
    GROUP BY
      trial_id,
      data_ingestion_timestamp
    """
)
trial_data_ingestion_latency_silver_df.createOrReplaceTempView("trial_data_ingestion_latency_silver")

(
    trial_data_ingestion_latency_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/trial_data_ingestion_latency_silver.csv")
)

job.commit()