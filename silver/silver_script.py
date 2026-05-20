import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# NOTE: getResolvedOptions requires at least one option name; if no job parameters are needed,
# avoid calling it to prevent runtime errors.

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# -----------------------------------------------------------------------------------
# 1) Read source tables (Bronze) and create temp views
# -----------------------------------------------------------------------------------
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

# ===================================================================================
# TABLE: clinical_trials_silver
# ===================================================================================
clinical_trials_silver_df = spark.sql(
    """
    WITH peb_clean AS (
      SELECT
        peb.trial_id AS trial_id,
        peb.site_id AS site_id,
        UPPER(TRIM(peb.country)) AS country,
        peb.enrollment_date AS enrollment_date
      FROM patient_enrollment_bronze peb
      WHERE peb.trial_id IS NOT NULL
        AND peb.site_id IS NOT NULL
    ),
    latest_non_null_country AS (
      SELECT
        trial_id,
        site_id,
        country,
        ROW_NUMBER() OVER (
          PARTITION BY trial_id, site_id
          ORDER BY
            CASE WHEN country IS NULL THEN 1 ELSE 0 END ASC,
            enrollment_date DESC
        ) AS rn
      FROM peb_clean
    )
    SELECT
      trial_id,
      site_id,
      country
    FROM latest_non_null_country
    WHERE rn = 1
    """
)

(
    clinical_trials_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trials_silver.csv")
)

# ===================================================================================
# TABLE: trial_enrollment_silver
# ===================================================================================
trial_enrollment_silver_df = spark.sql(
    """
    WITH peb_patient_latest AS (
      SELECT
        peb.trial_id AS trial_id,
        peb.site_id AS site_id,
        UPPER(TRIM(peb.country)) AS country,
        peb.patient_id AS patient_id,
        peb.consent_status AS consent_status,
        peb.enrollment_date AS enrollment_date,
        ROW_NUMBER() OVER (
          PARTITION BY peb.patient_id, peb.trial_id, peb.site_id
          ORDER BY peb.enrollment_date DESC
        ) AS rn
      FROM patient_enrollment_bronze peb
    ),
    peb_dedup AS (
      SELECT
        trial_id,
        site_id,
        country,
        patient_id,
        consent_status,
        enrollment_date
      FROM peb_patient_latest
      WHERE rn = 1
    )
    SELECT
      trial_id,
      site_id,
      country,
      COUNT(DISTINCT CASE WHEN UPPER(consent_status) IN ('CONSENTED','Y','YES','TRUE') THEN patient_id END) AS enrolled_patients_count,
      COUNT(DISTINCT CASE WHEN UPPER(consent_status) IN ('ACTIVE') THEN patient_id END) AS currently_active_patients,
      COUNT(DISTINCT CASE WHEN UPPER(consent_status) IN ('DROPPED','WITHDRAWN','INACTIVE') THEN patient_id END) AS patient_dropout_count,
      DATE(MAX(enrollment_date)) AS snapshot_date,
      MAX(enrollment_date) AS data_timestamp
    FROM peb_dedup
    GROUP BY trial_id, site_id, country
    """
)
trial_enrollment_silver_df.createOrReplaceTempView("trial_enrollment_silver")

(
    trial_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_enrollment_silver.csv")
)

# ===================================================================================
# TABLE: trial_progress_silver
# ===================================================================================
trial_progress_silver_df = spark.sql(
    """
    WITH cvb_dedup AS (
      SELECT
        cvb.visit_id AS visit_id,
        cvb.trial_id AS trial_id,
        cvb.patient_id AS patient_id,
        cvb.visit_type AS visit_type,
        cvb.visit_date AS visit_date,
        ROW_NUMBER() OVER (
          PARTITION BY cvb.visit_id
          ORDER BY cvb.visit_date DESC
        ) AS rn
      FROM clinical_visit_bronze cvb
    ),
    cvb_latest AS (
      SELECT
        visit_id,
        trial_id,
        patient_id,
        visit_type,
        visit_date
      FROM cvb_dedup
      WHERE rn = 1
    ),
    peb_patient_latest AS (
      SELECT
        peb.patient_id AS patient_id,
        peb.trial_id AS trial_id,
        peb.site_id AS site_id,
        UPPER(TRIM(peb.country)) AS country,
        peb.enrollment_date AS enrollment_date,
        ROW_NUMBER() OVER (
          PARTITION BY peb.patient_id, peb.trial_id
          ORDER BY peb.enrollment_date DESC
        ) AS rn
      FROM patient_enrollment_bronze peb
    ),
    peb_latest AS (
      SELECT
        patient_id,
        trial_id,
        site_id,
        country
      FROM peb_patient_latest
      WHERE rn = 1
    ),
    progress_base AS (
      SELECT
        cvb.trial_id AS trial_id,
        peb.site_id AS site_id,
        peb.country AS country,
        cvb.patient_id AS patient_id,
        cvb.visit_type AS visit_type,
        cvb.visit_date AS visit_date
      FROM cvb_latest cvb
      INNER JOIN peb_latest peb
        ON cvb.patient_id = peb.patient_id
       AND cvb.trial_id = peb.trial_id
    ),
    progress_agg AS (
      SELECT
        trial_id,
        site_id,
        country,
        (COUNT(DISTINCT CONCAT(patient_id,'|',visit_type)) / NULLIF(COUNT(DISTINCT patient_id), 0)) * 100 AS visit_completion_percentage,
        DATE(MAX(visit_date)) AS snapshot_date,
        MAX(visit_date) AS data_timestamp
      FROM progress_base
      GROUP BY trial_id, site_id, country
    ),
    trend_calc AS (
      SELECT
        tes.trial_id,
        tes.site_id,
        tes.country,
        tes.snapshot_date,
        (tes.enrolled_patients_count - LAG(tes.enrolled_patients_count) OVER (
          PARTITION BY tes.trial_id, tes.site_id, tes.country
          ORDER BY tes.snapshot_date
        )) AS enrollment_trend
      FROM trial_enrollment_silver tes
    )
    SELECT
      p.trial_id AS trial_id,
      p.site_id AS site_id,
      p.country AS country,
      p.visit_completion_percentage AS visit_completion_percentage,
      t.enrollment_trend AS enrollment_trend,
      p.snapshot_date AS snapshot_date,
      p.data_timestamp AS data_timestamp
    FROM progress_agg p
    LEFT JOIN trend_calc t
      ON p.trial_id = t.trial_id
     AND p.site_id = t.site_id
     AND p.country = t.country
     AND p.snapshot_date = t.snapshot_date
    """
)
trial_progress_silver_df.createOrReplaceTempView("trial_progress_silver")

(
    trial_progress_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_progress_silver.csv")
)

# ===================================================================================
# TABLE: kpi_metrics_silver
# ===================================================================================
kpi_metrics_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        tes.trial_id AS trial_id,
        tes.site_id AS site_id,
        tes.country AS country,
        tes.snapshot_date AS snapshot_date,
        tes.data_timestamp AS tes_data_timestamp,
        tps.data_timestamp AS tps_data_timestamp,
        tps.trial_id AS tps_trial_id,
        tps.snapshot_date AS tps_snapshot_date
      FROM trial_enrollment_silver tes
      LEFT JOIN trial_progress_silver tps
        ON tes.trial_id = tps.trial_id
       AND tes.site_id = tps.site_id
       AND tes.country = tps.country
       AND tes.snapshot_date = tps.snapshot_date
    ),
    null_stats AS (
      SELECT
        AVG(
          (
            CASE WHEN trial_id IS NULL THEN 1 ELSE 0 END +
            CASE WHEN site_id IS NULL THEN 1 ELSE 0 END +
            CASE WHEN country IS NULL THEN 1 ELSE 0 END +
            CASE WHEN tps_trial_id IS NULL THEN 1 ELSE 0 END +
            CASE WHEN tps_snapshot_date IS NULL THEN 1 ELSE 0 END
          ) / 5.0
        ) AS null_rate
      FROM joined
    ),
    dup_stats AS (
      SELECT
        (COUNT(*) - COUNT(DISTINCT CONCAT(COALESCE(trial_id,''),'|',COALESCE(site_id,''),'|',COALESCE(country,''),'|',COALESCE(CAST(snapshot_date AS STRING),'')))) / NULLIF(COUNT(*), 0) AS duplicate_rate
      FROM joined
    )
    SELECT
      CAST(hash(CONCAT(j.trial_id,'|',CAST(j.snapshot_date AS STRING))) AS STRING) AS metric_id,
      j.trial_id AS trial_id,
      CAST(((unix_timestamp(current_timestamp()) - unix_timestamp(greatest(j.tes_data_timestamp, j.tps_data_timestamp))) / 60) AS BIGINT) AS reporting_latency,
      CAST(((unix_timestamp(current_timestamp()) - unix_timestamp(greatest(j.tes_data_timestamp, j.tps_data_timestamp))) / 60) AS BIGINT) AS data_freshness,
      CAST(
        greatest(
          0D,
          least(
            100D,
            100D - (100D * (ns.null_rate + ds.duplicate_rate) / 2D)
          )
        ) AS DOUBLE
      ) AS data_quality_score,
      current_timestamp() AS evaluation_timestamp
    FROM joined j
    CROSS JOIN null_stats ns
    CROSS JOIN dup_stats ds
    """
)

(
    kpi_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/kpi_metrics_silver.csv")
)
