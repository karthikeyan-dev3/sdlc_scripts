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

# ----------------------------
# Read source tables (Bronze)
# ----------------------------
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

# ============================================================
# Table: silver.trial_site_patient_silver
# ============================================================
trial_site_patient_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        peb.patient_id AS patient_id,
        peb.trial_id AS trial_id,
        peb.site_id AS site_id,
        peb.country AS country,
        CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
        peb.consent_status AS consent_status,
        CASE
          WHEN peb.consent_status IN ('Consented','Y','Yes','True','1') THEN TRUE
          ELSE FALSE
        END AS enrolled_flag,
        ROW_NUMBER() OVER (
          PARTITION BY peb.patient_id, peb.trial_id
          ORDER BY CAST(peb.enrollment_date AS TIMESTAMP) DESC, peb.source_system DESC
        ) AS rn
      FROM patient_enrollment_bronze peb
    )
    SELECT
      patient_id,
      trial_id,
      site_id,
      country,
      enrollment_date,
      consent_status,
      enrolled_flag
    FROM ranked
    WHERE rn = 1
    """
)
trial_site_patient_silver_df.createOrReplaceTempView("trial_site_patient_silver")

(
    trial_site_patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_patient_silver.csv")
)

# ============================================================
# Table: silver.trial_site_visit_silver
# ============================================================
trial_site_visit_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        cvb.visit_id AS visit_id,
        cvb.patient_id AS patient_id,
        cvb.trial_id AS trial_id,
        tsps.site_id AS site_id,
        tsps.country AS country,
        CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
        cvb.visit_type AS visit_type,
        ROW_NUMBER() OVER (
          PARTITION BY cvb.visit_id
          ORDER BY CAST(cvb.visit_date AS TIMESTAMP) DESC, cvb.source_system DESC
        ) AS rn
      FROM clinical_visit_bronze cvb
      INNER JOIN trial_site_patient_silver tsps
        ON cvb.patient_id = tsps.patient_id
       AND cvb.trial_id = tsps.trial_id
    )
    SELECT
      visit_id,
      patient_id,
      trial_id,
      site_id,
      country,
      visit_date,
      visit_type
    FROM joined
    WHERE rn = 1
    """
)
trial_site_visit_silver_df.createOrReplaceTempView("trial_site_visit_silver")

(
    trial_site_visit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_visit_silver.csv")
)

# ============================================================
# Table: silver.trial_site_daily_kpis_silver
# ============================================================
trial_site_daily_kpis_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        tsps.trial_id AS trial_id,
        tsps.country AS country,
        tsps.site_id AS site_id,
        CAST(COALESCE(tsvs.visit_date, tsps.enrollment_date) AS DATE) AS date,
        tsps.enrolled_flag AS enrolled_flag,
        tsps.patient_id AS patient_id,
        tsvs.visit_date AS visit_date,
        tsvs.visit_id AS visit_id
      FROM trial_site_patient_silver tsps
      LEFT JOIN trial_site_visit_silver tsvs
        ON tsps.patient_id = tsvs.patient_id
       AND tsps.trial_id = tsvs.trial_id
    ),
    agg AS (
      SELECT
        trial_id,
        country,
        site_id,
        date,
        COUNT(DISTINCT CASE WHEN enrolled_flag THEN patient_id END) AS total_enrolled_patients,
        COUNT(DISTINCT CASE WHEN enrolled_flag AND (visit_date IS NOT NULL) THEN patient_id END) AS active_patients,
        COUNT(DISTINCT CASE WHEN 1 = 0 THEN patient_id END) AS patient_dropout_count,
        (
          (COUNT(DISTINCT visit_id) / NULLIF(COUNT(DISTINCT patient_id), 0)) * 100.0
        ) AS visit_completion_percentage
      FROM base
      GROUP BY trial_id, country, site_id, date
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
      (total_enrolled_patients - LAG(total_enrolled_patients) OVER (
        PARTITION BY trial_id, country, site_id
        ORDER BY date
      )) AS enrollment_trend
    FROM agg
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
# Table: silver.trial_site_current_kpis_silver
# ============================================================
trial_site_current_kpis_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        tsdks.trial_id AS trial_id,
        tsdks.country AS country,
        tsdks.site_id AS site_id,
        tsdks.total_enrolled_patients AS total_enrolled_patients,
        tsdks.active_patients AS active_patients,
        tsdks.patient_dropout_count AS patient_dropout_count,
        tsdks.visit_completion_percentage AS visit_completion_percentage,
        tsdks.enrollment_trend AS enrollment_trend,
        CURRENT_TIMESTAMP AS data_capture_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY tsdks.trial_id, tsdks.country, tsdks.site_id
          ORDER BY tsdks.date DESC
        ) AS rn
      FROM trial_site_daily_kpis_silver tsdks
    )
    SELECT
      trial_id,
      country,
      site_id,
      total_enrolled_patients,
      active_patients,
      patient_dropout_count,
      visit_completion_percentage,
      enrollment_trend,
      data_capture_timestamp
    FROM ranked
    WHERE rn = 1
    """
)
trial_site_current_kpis_silver_df.createOrReplaceTempView("trial_site_current_kpis_silver")

(
    trial_site_current_kpis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_current_kpis_silver.csv")
)

# ============================================================
# Table: silver.trial_site_aggregated_metrics_silver
# ============================================================
trial_site_aggregated_metrics_silver_df = spark.sql(
    """
    SELECT
      tsdks.trial_id AS trial_id,
      tsdks.country AS country,
      tsdks.site_id AS site_id,
      MAX(tsdks.total_enrolled_patients) AS aggregated_enrolment_count,
      SUM(tsdks.patient_dropout_count) AS aggregated_dropout_count,
      AVG(tsdks.visit_completion_percentage) AS aggregated_completion_rate,
      AVG(tsdks.enrollment_trend) AS aggregated_enrollment_trend
    FROM trial_site_daily_kpis_silver tsdks
    GROUP BY
      tsdks.trial_id,
      tsdks.country,
      tsdks.site_id
    """
)
trial_site_aggregated_metrics_silver_df.createOrReplaceTempView("trial_site_aggregated_metrics_silver")

(
    trial_site_aggregated_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_aggregated_metrics_silver.csv")
)

# ============================================================
# Table: silver.dashboard_metrics_silver
# ============================================================
dashboard_metrics_silver_df = spark.sql(
    """
    SELECT
      CAST(hash(concat('kpi', tscks.trial_id, tscks.country, tscks.site_id)) AS STRING) AS metric_id,
      concat('total_enrolled_patients:', tscks.trial_id) AS metric_name,
      CAST(tscks.total_enrolled_patients AS DOUBLE) AS value,
      tscks.data_capture_timestamp AS last_updated
    FROM trial_site_current_kpis_silver tscks

    UNION ALL

    SELECT
      CAST(hash(concat('kpi', tsams.trial_id, tsams.country, tsams.site_id)) AS STRING) AS metric_id,
      concat('total_enrolled_patients:', tsams.trial_id) AS metric_name,
      CAST(tsams.aggregated_enrolment_count AS DOUBLE) AS value,
      CURRENT_TIMESTAMP AS last_updated
    FROM trial_site_aggregated_metrics_silver tsams
    """
)

(
    dashboard_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dashboard_metrics_silver.csv")
)

job.commit()
