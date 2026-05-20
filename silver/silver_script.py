import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "visit_completion_threshold",
        "enrollment_trend_threshold",
        "expected_enrollment_day_threshold",
        "expected_enrollment_trend_threshold",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ======================================================================================
# Source Reads (Bronze)
# ======================================================================================
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
adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)

patient_enrollment_bronze_df.createOrReplaceTempView("patient_enrollment_bronze")
clinical_visit_bronze_df.createOrReplaceTempView("clinical_visit_bronze")
adverse_events_bronze_df.createOrReplaceTempView("adverse_events_bronze")

# ======================================================================================
# Target: silver.clinical_trial_site_day_silver
# ======================================================================================
clinical_trial_site_day_silver_sql = """
WITH
peb_dedup AS (
  SELECT
    peb.*
  FROM (
    SELECT
      peb.*,
      ROW_NUMBER() OVER (
        PARTITION BY peb.patient_id, peb.trial_id, peb.site_id, peb.enrollment_date
        ORDER BY peb.enrollment_date DESC
      ) AS rn
    FROM patient_enrollment_bronze peb
  ) peb
  WHERE peb.rn = 1
),
cvb_dedup AS (
  SELECT
    cvb.*
  FROM (
    SELECT
      cvb.*,
      ROW_NUMBER() OVER (
        PARTITION BY cvb.visit_id
        ORDER BY cvb.visit_date DESC
      ) AS rn
    FROM clinical_visit_bronze cvb
  ) cvb
  WHERE cvb.rn = 1
),
aeb_dedup AS (
  SELECT
    aeb.*
  FROM (
    SELECT
      aeb.*,
      ROW_NUMBER() OVER (
        PARTITION BY aeb.event_id
        ORDER BY aeb.event_start_date DESC
      ) AS rn
    FROM adverse_events_bronze aeb
  ) aeb
  WHERE aeb.rn = 1
),
base AS (
  SELECT
    peb.trial_id AS trial_id,
    peb.country AS country,
    peb.site_id AS site_id,
    CAST(peb.enrollment_date AS date) AS trend_date,
    peb.patient_id AS patient_id,
    aeb.outcome AS outcome,
    aeb.patient_id AS aeb_patient_id,
    CAST(aeb.event_start_date AS date) AS aeb_event_date,
    cvb.visit_id AS visit_id,
    CAST(cvb.visit_date AS date) AS visit_date
  FROM peb_dedup peb
  LEFT JOIN cvb_dedup cvb
    ON peb.patient_id = cvb.patient_id
   AND peb.trial_id = cvb.trial_id
  LEFT JOIN aeb_dedup aeb
    ON peb.patient_id = aeb.patient_id
),
daily AS (
  SELECT
    trial_id,
    country,
    site_id,
    trend_date,

    COUNT(DISTINCT patient_id) AS enrolled_patients_day,

    COUNT(DISTINCT CASE
      WHEN outcome NOT IN ('Dropped Out','Withdrawal','Withdrawn') OR outcome IS NULL
      THEN patient_id
    END) AS active_patients_day,

    COUNT(DISTINCT CASE
      WHEN outcome IN ('Dropped Out','Withdrawal','Withdrawn')
       AND aeb_event_date = trend_date
      THEN aeb_patient_id
    END) AS patient_dropout_count_day,

    COUNT(DISTINCT CASE
      WHEN visit_date = trend_date
      THEN visit_id
    END) AS visits_completed_day,

    COUNT(DISTINCT CASE
      WHEN outcome NOT IN ('Dropped Out','Withdrawal','Withdrawn') OR outcome IS NULL
      THEN patient_id
    END) AS expected_visits_day,

    (
      COUNT(DISTINCT CASE WHEN visit_date = trend_date THEN visit_id END) /
      NULLIF(
        COUNT(DISTINCT CASE
          WHEN outcome NOT IN ('Dropped Out','Withdrawal','Withdrawn') OR outcome IS NULL
          THEN patient_id
        END),
        0
      )
    ) AS visit_completion_percentage_day,

    CURRENT_TIMESTAMP AS data_refresh_time
  FROM base
  GROUP BY trial_id, country, site_id, trend_date
)
SELECT
  d.trial_id,
  d.country,
  d.site_id,
  d.trend_date,
  d.enrolled_patients_day,

  COUNT(DISTINCT d2.patient_id) OVER (
    PARTITION BY d.trial_id, d.country, d.site_id
    ORDER BY d.trend_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_total_enrolled_patients,

  d.active_patients_day,
  d.patient_dropout_count_day,

  COUNT(DISTINCT CASE
    WHEN d2.outcome IN ('Dropped Out','Withdrawal','Withdrawn')
    THEN d2.aeb_patient_id
  END) OVER (
    PARTITION BY d.trial_id, d.country, d.site_id
    ORDER BY d.trend_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_patient_dropout_count,

  d.visits_completed_day,
  d.expected_visits_day,
  d.visit_completion_percentage_day,

  AVG(d.enrolled_patients_day) OVER (
    PARTITION BY d.trial_id, d.country, d.site_id
    ORDER BY d.trend_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS enrollment_trend_value,

  AVG(d.patient_dropout_count_day) OVER (
    PARTITION BY d.trial_id, d.country, d.site_id
    ORDER BY d.trend_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS dropout_trend_value,

  AVG(d.visit_completion_percentage_day) OVER (
    PARTITION BY d.trial_id, d.country, d.site_id
    ORDER BY d.trend_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS visit_completion_trend_value,

  (
    1 - (
      COUNT(DISTINCT CASE
        WHEN d2.outcome IN ('Dropped Out','Withdrawal','Withdrawn')
        THEN d2.aeb_patient_id
      END) OVER (
        PARTITION BY d.trial_id, d.country, d.site_id
        ORDER BY d.trend_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
      /
      NULLIF(
        COUNT(DISTINCT d2.patient_id) OVER (
          PARTITION BY d.trial_id, d.country, d.site_id
          ORDER BY d.trend_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        0
      )
    )
  ) AS patient_retention_rate_day,

  (
    d.visits_completed_day / NULLIF(d.expected_visits_day, 0)
  ) AS visit_adherence_rate_day,

  d.data_refresh_time
FROM daily d
LEFT JOIN (
  SELECT
    trial_id,
    country,
    site_id,
    CAST(enrollment_date AS date) AS trend_date,
    patient_id,
    outcome,
    aeb_patient_id
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY b.patient_id, b.trial_id, b.site_id, b.trend_date, COALESCE(b.outcome, ''), COALESCE(b.aeb_patient_id, '')
        ORDER BY b.trend_date DESC
      ) AS rn
    FROM (
      SELECT
        peb.trial_id AS trial_id,
        peb.country AS country,
        peb.site_id AS site_id,
        peb.enrollment_date AS enrollment_date,
        peb.patient_id AS patient_id,
        aeb.outcome AS outcome,
        aeb.patient_id AS aeb_patient_id
      FROM (
        SELECT *
        FROM (
          SELECT
            peb.*,
            ROW_NUMBER() OVER (
              PARTITION BY peb.patient_id, peb.trial_id, peb.site_id, peb.enrollment_date
              ORDER BY peb.enrollment_date DESC
            ) AS rn
          FROM patient_enrollment_bronze peb
        ) x
        WHERE x.rn = 1
      ) peb
      LEFT JOIN (
        SELECT *
        FROM (
          SELECT
            aeb.*,
            ROW_NUMBER() OVER (
              PARTITION BY aeb.event_id
              ORDER BY aeb.event_start_date DESC
            ) AS rn
          FROM adverse_events_bronze aeb
        ) y
        WHERE y.rn = 1
      ) aeb
        ON peb.patient_id = aeb.patient_id
    ) b
  ) z
  WHERE z.rn = 1
) d2
  ON d.trial_id = d2.trial_id
 AND d.country = d2.country
 AND d.site_id = d2.site_id
 AND d.trend_date = d2.trend_date
"""

clinical_trial_site_day_silver_df = spark.sql(clinical_trial_site_day_silver_sql)

(
    clinical_trial_site_day_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_site_day_silver.csv")
)

clinical_trial_site_day_silver_df.createOrReplaceTempView("clinical_trial_site_day_silver")

# ======================================================================================
# Target: silver.clinical_trial_site_current_silver
# ======================================================================================
clinical_trial_site_current_silver_sql = f"""
SELECT
  ctsds.trial_id AS trial_id,
  ctsds.country AS country,
  ctsds.site_id AS site_id,
  ctsds.cumulative_total_enrolled_patients AS total_enrolled_patients,
  ctsds.active_patients_day AS active_patients,
  ctsds.cumulative_patient_dropout_count AS patient_dropout_count,
  ctsds.visit_completion_percentage_day AS visit_completion_percentage,
  ctsds.enrollment_trend_value AS enrollment_trend,
  ctsds.patient_retention_rate_day AS patient_retention_rate,
  ctsds.visit_adherence_rate_day AS visit_adherence_rate,
  (
    ctsds.visit_completion_percentage_day < CAST('{args["visit_completion_threshold"]}' AS double)
    OR ctsds.enrollment_trend_value < CAST('{args["enrollment_trend_threshold"]}' AS double)
  ) AS underperforming_sites_flag,
  (
    ctsds.enrolled_patients_day < CAST('{args["expected_enrollment_day_threshold"]}' AS bigint)
    OR ctsds.enrollment_trend_value < CAST('{args["expected_enrollment_trend_threshold"]}' AS double)
  ) AS enrollment_delay_flag,
  ctsds.data_refresh_time AS data_refresh_time
FROM (
  SELECT
    ctsds.*,
    ROW_NUMBER() OVER (
      PARTITION BY ctsds.trial_id, ctsds.country, ctsds.site_id
      ORDER BY ctsds.trend_date DESC
    ) AS rn
  FROM clinical_trial_site_day_silver ctsds
) ctsds
WHERE ctsds.rn = 1
"""

clinical_trial_site_current_silver_df = spark.sql(clinical_trial_site_current_silver_sql)

(
    clinical_trial_site_current_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_site_current_silver.csv")
)

job.commit()
