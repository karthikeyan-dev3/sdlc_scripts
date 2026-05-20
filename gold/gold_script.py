import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Source: silver.trial_site_daily_metrics_silver (tsdm)
# ----------------------------
tsdm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_daily_metrics_silver.{FILE_FORMAT}/")
)
tsdm_df.createOrReplaceTempView("trial_site_daily_metrics_silver")

# ============================================================
# Target: gold.gold_clinical_trial_metrics
# ============================================================
gold_clinical_trial_metrics_sql = """
SELECT
  CAST(tsdm.trial_id AS STRING) AS trial_id,
  CAST(tsdm.country AS STRING) AS country,
  CAST(tsdm.site_id AS STRING) AS site_id,
  DATE(CAST(tsdm.metric_date AS DATE)) AS data_capture_date,
  CAST(tsdm.total_enrolled_patients AS INT) AS total_enrolled_patients,
  CAST(tsdm.active_patients AS INT) AS active_patients,
  CAST(tsdm.patient_dropout_count AS INT) AS patient_dropout_count,
  CAST(tsdm.visit_completion_percentage AS DECIMAL(38,18)) AS visit_completion_percentage,
  CAST(
    CAST(tsdm.total_enrolled_patients AS INT)
    - LAG(CAST(tsdm.total_enrolled_patients AS INT)) OVER (
        PARTITION BY tsdm.trial_id, tsdm.country, tsdm.site_id
        ORDER BY CAST(tsdm.metric_date AS DATE)
      )
    AS INT
  ) AS enrollment_trend
FROM trial_site_daily_metrics_silver tsdm
"""

gold_clinical_trial_metrics_df = spark.sql(gold_clinical_trial_metrics_sql)

(
    gold_clinical_trial_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_metrics.csv")
)

# ============================================================
# Target: gold.gold_clinical_trial_trends
# ============================================================
gold_clinical_trial_trends_sql = """
SELECT
  CAST(tsdm.trial_id AS STRING) AS trial_id,
  CAST(tsdm.country AS STRING) AS country,
  CAST(tsdm.site_id AS STRING) AS site_id,
  DATE(CAST(tsdm.metric_date AS DATE)) AS trend_date,
  CAST('ENROLLMENT_TOTAL' AS STRING) AS metric_name,
  CAST(tsdm.total_enrolled_patients AS DECIMAL(38,18)) AS metric_value
FROM trial_site_daily_metrics_silver tsdm

UNION ALL

SELECT
  CAST(tsdm.trial_id AS STRING) AS trial_id,
  CAST(tsdm.country AS STRING) AS country,
  CAST(tsdm.site_id AS STRING) AS site_id,
  DATE(CAST(tsdm.metric_date AS DATE)) AS trend_date,
  CAST('ACTIVE_PATIENTS' AS STRING) AS metric_name,
  CAST(tsdm.active_patients AS DECIMAL(38,18)) AS metric_value
FROM trial_site_daily_metrics_silver tsdm

UNION ALL

SELECT
  CAST(tsdm.trial_id AS STRING) AS trial_id,
  CAST(tsdm.country AS STRING) AS country,
  CAST(tsdm.site_id AS STRING) AS site_id,
  DATE(CAST(tsdm.metric_date AS DATE)) AS trend_date,
  CAST('DROPOUT_COUNT' AS STRING) AS metric_name,
  CAST(tsdm.patient_dropout_count AS DECIMAL(38,18)) AS metric_value
FROM trial_site_daily_metrics_silver tsdm

UNION ALL

SELECT
  CAST(tsdm.trial_id AS STRING) AS trial_id,
  CAST(tsdm.country AS STRING) AS country,
  CAST(tsdm.site_id AS STRING) AS site_id,
  DATE(CAST(tsdm.metric_date AS DATE)) AS trend_date,
  CAST('VISIT_COMPLETION_PCT' AS STRING) AS metric_name,
  CAST(tsdm.visit_completion_percentage AS DECIMAL(38,18)) AS metric_value
FROM trial_site_daily_metrics_silver tsdm
"""

gold_clinical_trial_trends_df = spark.sql(gold_clinical_trial_trends_sql)

(
    gold_clinical_trial_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_trends.csv")
)

# ============================================================
# Target: gold.gold_clinical_trial_performance_summary
# Note: business thresholds referenced in UDT are not provided as literals/columns.
#       Using NULL casts to keep only specified columns without inventing new inputs.
# ============================================================
gold_clinical_trial_performance_summary_sql = """
WITH ranked AS (
  SELECT
    tsdm.*,
    ROW_NUMBER() OVER (
      PARTITION BY tsdm.trial_id, tsdm.country, tsdm.site_id
      ORDER BY CAST(tsdm.metric_date AS DATE) DESC
    ) AS rn_desc,
    (
      CAST(tsdm.total_enrolled_patients AS INT)
      - LAG(CAST(tsdm.total_enrolled_patients AS INT)) OVER (
          PARTITION BY tsdm.trial_id, tsdm.country, tsdm.site_id
          ORDER BY CAST(tsdm.metric_date AS DATE)
        )
    ) AS enrollment_velocity
  FROM trial_site_daily_metrics_silver tsdm
),
latest AS (
  SELECT
    trial_id,
    country,
    site_id,
    enrollment_velocity,
    CAST(patient_dropout_count AS DECIMAL(38,18)) / NULLIF(CAST(total_enrolled_patients AS DECIMAL(38,18)), CAST(0 AS DECIMAL(38,18))) AS dropout_rate,
    CAST(visit_completion_percentage AS DECIMAL(38,18)) AS visit_completion_percentage
  FROM ranked
  WHERE rn_desc = 1
)
SELECT
  CAST(trial_id AS STRING) AS trial_id,
  CAST(country AS STRING) AS country,
  CAST(site_id AS STRING) AS site_id,
  CAST(NULL AS BOOLEAN) AS underperforming_flag,
  CAST(NULL AS BOOLEAN) AS enrollment_delay_flag,
  CAST(NULL AS BOOLEAN) AS patient_retention_flag,
  CAST(NULL AS BOOLEAN) AS visit_adherence_flag
FROM latest
"""

gold_clinical_trial_performance_summary_df = spark.sql(gold_clinical_trial_performance_summary_sql)

(
    gold_clinical_trial_performance_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_performance_summary.csv")
)
