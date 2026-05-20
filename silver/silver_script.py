import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) READ SOURCE TABLES (S3)
# ----------------------------
clinical_trials_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trials_bronze.{FILE_FORMAT}/")
)
sites_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sites_bronze.{FILE_FORMAT}/")
)
countries_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/countries_bronze.{FILE_FORMAT}/")
)
data_management_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_management_bronze.{FILE_FORMAT}/")
)
metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/metrics_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# 2) CREATE TEMP VIEWS
# ----------------------------
clinical_trials_bronze_df.createOrReplaceTempView("clinical_trials_bronze")
sites_bronze_df.createOrReplaceTempView("sites_bronze")
countries_bronze_df.createOrReplaceTempView("countries_bronze")
data_management_bronze_df.createOrReplaceTempView("data_management_bronze")
metrics_bronze_df.createOrReplaceTempView("metrics_bronze")

# ============================================================
# TARGET 1: silver.trial_site_patient_enrollment_silver
# ============================================================
trial_site_patient_enrollment_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        ctb.trial_id AS trial_id,
        ctb.site_id AS site_id,
        ctb.patient_id AS patient_id,
        ctb.enrollment_date AS enrollment_date,
        ctb.consent_status AS consent_status,
        ctb.source_system AS source_system,
        ctb.country AS country
      FROM clinical_trials_bronze ctb
      LEFT JOIN sites_bronze sb
        ON ctb.trial_id = sb.trial_id
       AND ctb.site_id = sb.site_id
       AND ctb.patient_id = sb.patient_id
      LEFT JOIN countries_bronze cob
        ON ctb.trial_id = cob.trial_id
       AND ctb.site_id = cob.site_id
       AND ctb.patient_id = cob.patient_id
    ),
    dedup AS (
      SELECT
        trial_id,
        site_id,
        patient_id,
        enrollment_date,
        consent_status,
        source_system,
        country,
        UPPER(TRIM(country)) AS country_norm,
        CAST(enrollment_date AS timestamp) AS enrollment_date_ts,
        (consent_status IN ('CONSENTED','Y','YES','TRUE','1')) AS is_consented,
        ROW_NUMBER() OVER (
          PARTITION BY trial_id, site_id, patient_id, source_system
          ORDER BY CAST(enrollment_date AS timestamp) DESC
        ) AS rn
      FROM joined
    )
    SELECT
      trial_id,
      site_id,
      patient_id,
      enrollment_date,
      consent_status,
      source_system,
      country,
      country_norm,
      enrollment_date_ts,
      is_consented
    FROM dedup
    WHERE rn = 1
    """
)
trial_site_patient_enrollment_silver_df.createOrReplaceTempView("trial_site_patient_enrollment_silver")

(
    trial_site_patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_patient_enrollment_silver.csv")
)

# ============================================================
# TARGET 2: silver.trial_site_patient_status_silver
# ============================================================
dmb_dedup_df = spark.sql(
    """
    WITH d AS (
      SELECT
        dmb.trial_id,
        dmb.patient_id,
        dmb.visit_id,
        dmb.visit_date,
        ROW_NUMBER() OVER (
          PARTITION BY dmb.visit_id
          ORDER BY CAST(dmb.visit_date AS timestamp) DESC
        ) AS rn
      FROM data_management_bronze dmb
    )
    SELECT
      trial_id,
      patient_id,
      visit_id,
      visit_date
    FROM d
    WHERE rn = 1
    """
)
dmb_dedup_df.createOrReplaceTempView("dmb_dedup")

trial_site_patient_status_silver_df = spark.sql(
    """
    SELECT
      tspe.trial_id AS trial_id,
      tspe.site_id AS site_id,
      tspe.patient_id AS patient_id,
      MAX(dmb.visit_date) AS last_visit_date,
      CAST(COUNT(DISTINCT dmb.visit_id) AS int) AS visit_count,
      ((MAX(dmb.visit_date) >= (CURRENT_DATE - INTERVAL 90 DAYS)) AND (tspe.is_consented = true)) AS is_active
    FROM trial_site_patient_enrollment_silver tspe
    LEFT JOIN dmb_dedup dmb
      ON tspe.trial_id = dmb.trial_id
     AND tspe.patient_id = dmb.patient_id
    GROUP BY
      tspe.trial_id,
      tspe.site_id,
      tspe.patient_id,
      tspe.is_consented
    """
)
trial_site_patient_status_silver_df.createOrReplaceTempView("trial_site_patient_status_silver")

(
    trial_site_patient_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_patient_status_silver.csv")
)

# ============================================================
# TARGET 3: silver.trial_site_visit_expected_silver
# ============================================================
trial_site_visit_expected_silver_df = spark.sql(
    """
    SELECT
      tspe.trial_id AS trial_id,
      tspe.site_id AS site_id,
      tspe.patient_id AS patient_id,
      CAST(GREATEST(FLOOR(DATEDIFF(CURRENT_DATE, CAST(tspe.enrollment_date_ts AS date)) / 30) + 1, 0) AS int) AS expected_visits_to_date
    FROM trial_site_patient_enrollment_silver tspe
    """
)
trial_site_visit_expected_silver_df.createOrReplaceTempView("trial_site_visit_expected_silver")

(
    trial_site_visit_expected_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_visit_expected_silver.csv")
)

# ============================================================
# TARGET 4: silver.trial_site_daily_kpis_silver
# ============================================================
trial_site_daily_kpis_silver_df = spark.sql(
    """
    SELECT
      tspe.trial_id AS trial_id,
      tspe.country_norm AS country,
      tspe.site_id AS site_id,
      CAST(COUNT(DISTINCT CASE WHEN tspe.is_consented = true THEN tspe.patient_id END) AS int) AS total_enrolled_patients,
      CAST(COUNT(DISTINCT CASE WHEN tsps.is_active = true THEN tsps.patient_id END) AS int) AS active_patients,
      CAST(SUM(tsps.visit_count) AS int) AS actual_visits,
      CAST(SUM(tsve.expected_visits_to_date) AS int) AS expected_visits_to_date,
      CASE
        WHEN SUM(tsve.expected_visits_to_date) > 0
          THEN (SUM(tsps.visit_count) / SUM(tsve.expected_visits_to_date)) * 100
      END AS visit_completion_percentage
    FROM trial_site_patient_enrollment_silver tspe
    LEFT JOIN trial_site_patient_status_silver tsps
      ON tspe.trial_id = tsps.trial_id
     AND tspe.site_id = tsps.site_id
     AND tspe.patient_id = tsps.patient_id
    LEFT JOIN trial_site_visit_expected_silver tsve
      ON tspe.trial_id = tsve.trial_id
     AND tspe.site_id = tsve.site_id
     AND tspe.patient_id = tsve.patient_id
    GROUP BY
      tspe.trial_id,
      tspe.country_norm,
      tspe.site_id
    """
)
trial_site_daily_kpis_silver_df.createOrReplaceTempView("trial_site_daily_kpis_silver")

(
    trial_site_daily_kpis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_daily_kpis_silver.csv")
)

# ============================================================
# TARGET 5: silver.trial_site_performance_flags_silver
# ============================================================
trial_site_performance_flags_silver_df = spark.sql(
    """
    WITH min_enroll AS (
      SELECT
        trial_id,
        site_id,
        MIN(enrollment_date_ts) AS min_enrollment_date_ts
      FROM trial_site_patient_enrollment_silver
      GROUP BY trial_id, site_id
    )
    SELECT
      tsdk.trial_id AS trial_id,
      tsdk.site_id AS site_id,
      tsdk.country AS country,
      (
        (tsdk.active_patients / NULLIF(tsdk.total_enrolled_patients, 0) < 0.5)
        OR (tsdk.visit_completion_percentage < 70)
      ) AS underperforming_site_flag,
      (
        tsdk.total_enrolled_patients = 0
        AND DATEDIFF(CURRENT_DATE, CAST(me.min_enrollment_date_ts AS date)) > 30
      ) AS enrollment_delay_flag
    FROM trial_site_daily_kpis_silver tsdk
    LEFT JOIN min_enroll me
      ON tsdk.trial_id = me.trial_id
     AND tsdk.site_id = me.site_id
    """
)
trial_site_performance_flags_silver_df.createOrReplaceTempView("trial_site_performance_flags_silver")

(
    trial_site_performance_flags_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_performance_flags_silver.csv")
)

# ============================================================
# TARGET 6: silver.trial_site_metric_trends_silver
# Note: Only columns explicitly provided in UDT columns list are produced.
# ============================================================
trial_site_metric_trends_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        mb.patient_id AS patient_id,
        tspe.trial_id AS trial_id,
        tspe.site_id AS site_id,
        tspe.country_norm AS country,
        mb.recorded_timestamp AS metric_timestamp,
        CAST(mb.recorded_timestamp AS date) AS metric_date,
        ROW_NUMBER() OVER (
          PARTITION BY mb.patient_id
          ORDER BY CAST(mb.recorded_timestamp AS timestamp) DESC
        ) AS rn
      FROM metrics_bronze mb
      INNER JOIN trial_site_patient_enrollment_silver tspe
        ON mb.patient_id = tspe.patient_id
    )
    SELECT
      patient_id,
      trial_id,
      site_id,
      country,
      metric_timestamp,
      metric_date
    FROM joined
    WHERE rn = 1
    """
)
trial_site_metric_trends_silver_df.createOrReplaceTempView("trial_site_metric_trends_silver")

(
    trial_site_metric_trends_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_metric_trends_silver.csv")
)

# ============================================================
# TARGET 7: silver.data_freshness_quality_silver
# ============================================================
data_freshness_quality_silver_df = spark.sql(
    """
    WITH d AS (
      SELECT
        dmb.visit_id,
        dmb.patient_id,
        dmb.trial_id,
        dmb.visit_date,
        ROW_NUMBER() OVER (
          PARTITION BY dmb.visit_id
          ORDER BY CAST(dmb.visit_date AS timestamp) DESC
        ) AS rn
      FROM data_management_bronze dmb
    ),
    d_dedup AS (
      SELECT
        visit_id,
        patient_id,
        trial_id,
        visit_date
      FROM d
      WHERE rn = 1
    )
    SELECT
      MAX(visit_date) AS data_update_time,
      CAST((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(visit_date))) / 60 AS int) AS reporting_latency,
      ROUND(
        100 * (
          0.7 * AVG(
            CASE
              WHEN visit_id IS NOT NULL
               AND patient_id IS NOT NULL
               AND trial_id IS NOT NULL
               AND visit_date IS NOT NULL
              THEN 1 ELSE 0
            END
          )
          + 0.3 * (
            1 - ((COUNT(*) - COUNT(DISTINCT visit_id)) / NULLIF(COUNT(*), 0))
          )
        ),
        2
      ) AS data_quality_score
    FROM d_dedup
    """
)
data_freshness_quality_silver_df.createOrReplaceTempView("data_freshness_quality_silver")

(
    data_freshness_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_freshness_quality_silver.csv")
)