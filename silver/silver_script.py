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

# -------------------------
# 1) patient_silver
# -------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
peb_df.createOrReplaceTempView("patient_enrollment_bronze")

patient_silver_df = spark.sql(
    """
WITH ranked AS (
  SELECT
    peb.patient_id AS patient_id,
    peb.trial_id AS trial_id,
    peb.site_id AS site_id,
    peb.patient_name AS patient_name,
    peb.gender AS gender,
    CAST(peb.date_of_birth AS DATE) AS date_of_birth,
    peb.country AS country,
    CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
    peb.consent_status AS consent_status,
    peb.source_system AS source_system,
    ROW_NUMBER() OVER (
      PARTITION BY peb.patient_id
      ORDER BY CAST(peb.enrollment_date AS TIMESTAMP) DESC, peb.source_system DESC
    ) AS rn
  FROM patient_enrollment_bronze peb
)
SELECT
  patient_id,
  trial_id,
  site_id,
  patient_name,
  gender,
  date_of_birth,
  country,
  enrollment_date,
  consent_status,
  source_system
FROM ranked
WHERE rn = 1
"""
)
patient_silver_df.createOrReplaceTempView("patient_silver")

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

# -------------------------
# 2) lab_results_silver
# -------------------------
lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
lrb_df.createOrReplaceTempView("lab_results_bronze")

lab_results_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    lrb.lab_result_id AS lab_result_id,
    lrb.patient_id AS patient_id,
    lrb.sample_id AS sample_id,
    lrb.test_name AS test_name,
    CASE
      WHEN UPPER(TRIM(lrb.test_name)) LIKE '%HBA1C%' THEN 'HBA1C'
      WHEN UPPER(TRIM(lrb.test_name)) LIKE '%GLUCOSE%' THEN 'GLUCOSE'
      ELSE UPPER(TRIM(lrb.test_name))
    END AS test_code,
    CAST(lrb.test_result AS DOUBLE) AS test_result,
    lrb.test_unit AS test_unit,
    lrb.reference_range AS reference_range,
    CAST(lrb.abnormal_flag AS BOOLEAN) AS abnormal_flag,
    CAST(lrb.test_date AS TIMESTAMP) AS test_date,
    lrb.lab_name AS lab_name
  FROM lab_results_bronze lrb
  INNER JOIN patient_silver ps
    ON ps.patient_id = lrb.patient_id
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, sample_id, test_name, test_date, test_result, test_unit, lab_name
      ORDER BY
        CASE WHEN reference_range IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN abnormal_flag IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  lab_result_id,
  patient_id,
  sample_id,
  test_name,
  test_code,
  test_result,
  test_unit,
  reference_range,
  abnormal_flag,
  test_date,
  lab_name
FROM ranked
WHERE rn = 1
"""
)
lab_results_silver_df.createOrReplaceTempView("lab_results_silver")

(
    lab_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_silver.csv")
)

# -------------------------
# 3) wearable_monitoring_silver
# -------------------------
wmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)
wmb_df.createOrReplaceTempView("wearable_monitoring_bronze")

wearable_monitoring_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    wmb.device_record_id AS device_record_id,
    wmb.patient_id AS patient_id,
    wmb.device_type AS device_type,
    CAST(wmb.recorded_timestamp AS TIMESTAMP) AS recorded_timestamp,
    CAST(wmb.glucose_level AS DOUBLE) AS glucose_level,
    CAST(wmb.step_count AS INT) AS step_count,
    CAST(wmb.sleep_hours AS DOUBLE) AS sleep_hours,
    CAST(wmb.heart_rate AS DOUBLE) AS heart_rate,
    wmb.battery_status AS battery_status
  FROM wearable_monitoring_bronze wmb
  INNER JOIN patient_silver ps
    ON ps.patient_id = wmb.patient_id
),
filtered AS (
  SELECT *
  FROM base
  WHERE recorded_timestamp IS NOT NULL
    AND (step_count IS NULL OR step_count >= 0)
    AND (glucose_level IS NULL OR glucose_level >= 0)
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, device_type, recorded_timestamp
      ORDER BY
        (CASE WHEN glucose_level IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN heart_rate IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN sleep_hours IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN step_count IS NOT NULL THEN 1 ELSE 0 END) DESC
    ) AS rn
  FROM filtered
)
SELECT
  device_record_id,
  patient_id,
  device_type,
  recorded_timestamp,
  glucose_level,
  step_count,
  sleep_hours,
  heart_rate,
  battery_status
FROM ranked
WHERE rn = 1
"""
)
wearable_monitoring_silver_df.createOrReplaceTempView("wearable_monitoring_silver")

(
    wearable_monitoring_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_monitoring_silver.csv")
)

# -------------------------
# 4) patient_daily_health_indicators_silver
# -------------------------
patient_daily_health_indicators_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    ps.patient_id AS patient_id,
    COALESCE(DATE(lrs.test_date), DATE(wms.recorded_timestamp)) AS date,
    lrs.test_code AS test_code,
    lrs.test_result AS lab_test_result,
    wms.glucose_level AS wearable_glucose_level
  FROM patient_silver ps
  LEFT JOIN lab_results_silver lrs
    ON ps.patient_id = lrs.patient_id
  LEFT JOIN wearable_monitoring_silver wms
    ON ps.patient_id = wms.patient_id
),
agg AS (
  SELECT
    patient_id,
    date,
    AVG(CASE WHEN test_code = 'HBA1C' THEN lab_test_result END) AS hbA1c_level,
    COALESCE(
      AVG(wearable_glucose_level),
      AVG(CASE WHEN test_code = 'GLUCOSE' THEN lab_test_result END)
    ) AS glucose_level
  FROM base
  WHERE date IS NOT NULL
  GROUP BY patient_id, date
)
SELECT
  patient_id,
  date,
  hbA1c_level,
  glucose_level
FROM agg
"""
)
patient_daily_health_indicators_silver_df.createOrReplaceTempView(
    "patient_daily_health_indicators_silver"
)

(
    patient_daily_health_indicators_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_daily_health_indicators_silver.csv")
)

# -------------------------
# 5) abnormal_results_silver
# -------------------------
abnormal_results_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    lrs.patient_id AS patient_id,
    CONCAT('ABNORMAL_', lrs.test_code) AS abnormal_result_type,
    lrs.test_code AS test_code,
    lrs.test_name AS test_name,
    lrs.test_date AS test_date,
    lrs.test_result AS test_result,
    lrs.lab_name AS lab_name,
    lrs.abnormal_flag AS abnormal_flag,
    ROW_NUMBER() OVER (
      PARTITION BY lrs.patient_id, lrs.test_code, lrs.test_date, lrs.test_result, lrs.lab_name
      ORDER BY lrs.test_date DESC
    ) AS rn
  FROM lab_results_silver lrs
  INNER JOIN patient_silver ps
    ON ps.patient_id = lrs.patient_id
  WHERE lrs.abnormal_flag = TRUE
)
SELECT
  patient_id,
  abnormal_result_type,
  test_code,
  test_name,
  test_date
FROM base
WHERE rn = 1
"""
)
abnormal_results_silver_df.createOrReplaceTempView("abnormal_results_silver")

(
    abnormal_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/abnormal_results_silver.csv")
)

# -------------------------
# 6) patient_response_features_silver
# -------------------------
patient_response_features_silver_df = spark.sql(
    """
SELECT
  pdhis.patient_id AS patient_id,
  pdhis.date AS date,
  CASE
    WHEN pdhis.hbA1c_level IS NOT NULL THEN 'HBA1C_TREND_BASED'
    WHEN pdhis.glucose_level IS NOT NULL THEN 'GLUCOSE_TREND_BASED'
  END AS response_pattern,
  CASE
    WHEN pdhis.glucose_level IS NOT NULL THEN 'CHANGE_DETECTED'
  END AS health_condition_change,
  CASE
    WHEN pdhis.hbA1c_level IS NOT NULL THEN 'INDICATOR_TREND'
    WHEN pdhis.glucose_level IS NOT NULL THEN 'INDICATOR_TREND'
  END AS trend_analysis
FROM patient_daily_health_indicators_silver pdhis
INNER JOIN patient_silver ps
  ON ps.patient_id = pdhis.patient_id
"""
)
patient_response_features_silver_df.createOrReplaceTempView(
    "patient_response_features_silver"
)

(
    patient_response_features_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_response_features_silver.csv")
)

# -------------------------
# 7) integrated_health_data_silver
# -------------------------
integrated_health_data_silver_df = spark.sql(
    """
WITH joined AS (
  SELECT
    pdhis.patient_id AS patient_id,
    pdhis.date AS date,
    lrs.test_name AS lrs_test_name,
    lrs.test_result AS lrs_test_result,
    lrs.test_unit AS lrs_test_unit,
    lrs.abnormal_flag AS lrs_abnormal_flag,
    lrs.test_date AS lrs_test_date,
    lrs.lab_name AS lrs_lab_name,
    wms.device_type AS wms_device_type,
    wms.recorded_timestamp AS wms_recorded_timestamp,
    wms.glucose_level AS wms_glucose_level,
    wms.step_count AS wms_step_count,
    wms.sleep_hours AS wms_sleep_hours,
    wms.heart_rate AS wms_heart_rate,
    wms.battery_status AS wms_battery_status,
    ps.trial_id AS ps_trial_id,
    ps.site_id AS ps_site_id,
    ps.gender AS ps_gender,
    ps.date_of_birth AS ps_date_of_birth,
    ps.country AS ps_country,
    ps.enrollment_date AS ps_enrollment_date,
    ps.consent_status AS ps_consent_status,
    ps.source_system AS ps_source_system
  FROM patient_silver ps
  INNER JOIN patient_daily_health_indicators_silver pdhis
    ON ps.patient_id = pdhis.patient_id
  LEFT JOIN lab_results_silver lrs
    ON ps.patient_id = lrs.patient_id
   AND DATE(lrs.test_date) = pdhis.date
  LEFT JOIN wearable_monitoring_silver wms
    ON ps.patient_id = wms.patient_id
   AND DATE(wms.recorded_timestamp) = pdhis.date
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, date
      ORDER BY
        CASE WHEN lrs_test_date IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN wms_recorded_timestamp IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM joined
)
SELECT
  patient_id,
  date,
  to_json(named_struct(
    'test_name', lrs_test_name,
    'test_result', lrs_test_result,
    'test_unit', lrs_test_unit,
    'abnormal_flag', lrs_abnormal_flag,
    'test_date', lrs_test_date,
    'lab_name', lrs_lab_name
  )) AS laboratory_data,
  to_json(named_struct(
    'device_type', wms_device_type,
    'recorded_timestamp', wms_recorded_timestamp,
    'glucose_level', wms_glucose_level,
    'step_count', wms_step_count,
    'sleep_hours', wms_sleep_hours,
    'heart_rate', wms_heart_rate,
    'battery_status', wms_battery_status
  )) AS wearable_data,
  to_json(named_struct(
    'trial_id', ps_trial_id,
    'site_id', ps_site_id,
    'gender', ps_gender,
    'date_of_birth', ps_date_of_birth,
    'country', ps_country,
    'enrollment_date', ps_enrollment_date,
    'consent_status', ps_consent_status,
    'source_system', ps_source_system
  )) AS patient_related_data
FROM ranked
WHERE rn = 1
"""
)
integrated_health_data_silver_df.createOrReplaceTempView("integrated_health_data_silver")

(
    integrated_health_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/integrated_health_data_silver.csv")
)

job.commit()
