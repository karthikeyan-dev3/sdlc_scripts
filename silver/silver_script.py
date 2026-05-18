import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [])
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================
# READ BRONZE SOURCES (S3)
# =========================
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
peb_df.createOrReplaceTempView("patient_enrollment_bronze")

lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
lrb_df.createOrReplaceTempView("lab_results_bronze")

wmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)
wmb_df.createOrReplaceTempView("wearable_monitoring_bronze")

dab_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
dab_df.createOrReplaceTempView("drug_administration_bronze")

# ==========================================
# TABLE: patient_demographics_silver (pds)
# ==========================================
pds_sql = """
WITH base AS (
  SELECT
    TRIM(peb.patient_id) AS patient_id,
    peb.trial_id AS trial_id,
    peb.site_id AS site_id,
    peb.patient_name AS patient_name,
    peb.gender AS gender,
    CAST(peb.date_of_birth AS DATE) AS date_of_birth,
    peb.country AS country,
    CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
    peb.consent_status AS consent_status,
    peb.source_system AS source_system
  FROM patient_enrollment_bronze peb
  WHERE peb.patient_id IS NOT NULL
),
dedup AS (
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
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id
      ORDER BY enrollment_date DESC, source_system DESC
    ) AS rn
  FROM base
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
FROM dedup
WHERE rn = 1
"""
pds_df = spark.sql(pds_sql)
pds_df.createOrReplaceTempView("patient_demographics_silver")

(
    pds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_demographics_silver.csv")
)

# =================================
# TABLE: lab_results_silver (lrs)
# =================================
lrs_sql = """
WITH joined AS (
  SELECT
    lrb.patient_id AS patient_id,
    lrb.lab_result_id AS lab_result_id,
    CAST(lrb.test_date AS TIMESTAMP) AS test_date,
    UPPER(TRIM(lrb.test_name)) AS test_type,
    CAST(lrb.test_result AS DOUBLE) AS test_result_value,
    NULLIF(TRIM(lrb.test_unit), '') AS result_unit,
    lrb.abnormal_flag AS abnormal_flag
  FROM lab_results_bronze lrb
  INNER JOIN patient_enrollment_bronze peb
    ON lrb.patient_id = peb.patient_id
  WHERE lrb.patient_id IS NOT NULL
    AND lrb.lab_result_id IS NOT NULL
),
dedup AS (
  SELECT
    patient_id,
    lab_result_id,
    test_date,
    test_type,
    test_result_value,
    result_unit,
    abnormal_flag,
    ROW_NUMBER() OVER (
      PARTITION BY lab_result_id
      ORDER BY test_date DESC
    ) AS rn
  FROM joined
)
SELECT
  patient_id,
  lab_result_id,
  test_date,
  test_type,
  test_result_value,
  result_unit,
  abnormal_flag
FROM dedup
WHERE rn = 1
"""
lrs_df = spark.sql(lrs_sql)
lrs_df.createOrReplaceTempView("lab_results_silver")

(
    lrs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_silver.csv")
)

# ==========================================
# TABLE: wearable_daily_summary_silver (wdss)
# ==========================================
wdss_sql = """
WITH dedup AS (
  SELECT
    wmb.patient_id AS patient_id,
    CAST(wmb.recorded_timestamp AS TIMESTAMP) AS recorded_timestamp,
    DATE(CAST(wmb.recorded_timestamp AS TIMESTAMP)) AS wearable_date,
    CAST(wmb.glucose_level AS DOUBLE) AS glucose_level,
    CAST(wmb.step_count AS BIGINT) AS step_count,
    CAST(wmb.sleep_hours AS DOUBLE) AS sleep_hours,
    CAST(wmb.heart_rate AS DOUBLE) AS heart_rate,
    ROW_NUMBER() OVER (
      PARTITION BY wmb.patient_id, wmb.device_record_id
      ORDER BY CAST(wmb.recorded_timestamp AS TIMESTAMP) DESC
    ) AS rn
  FROM wearable_monitoring_bronze wmb
  WHERE wmb.patient_id IS NOT NULL
),
filtered AS (
  SELECT
    patient_id,
    wearable_date,
    glucose_level,
    step_count,
    sleep_hours,
    heart_rate
  FROM dedup
  WHERE rn = 1
    AND (glucose_level IS NULL OR glucose_level >= 0)
    AND (step_count IS NULL OR step_count >= 0)
    AND (sleep_hours IS NULL OR sleep_hours >= 0)
    AND (heart_rate IS NULL OR heart_rate >= 0)
)
SELECT
  patient_id,
  wearable_date,
  AVG(glucose_level) AS glucose_avg,
  MIN(glucose_level) AS glucose_min,
  MAX(glucose_level) AS glucose_max,
  CAST(SUM(step_count) AS INT) AS steps_total,
  SUM(sleep_hours) AS sleep_hours_total,
  AVG(heart_rate) AS heart_rate_avg
FROM filtered
GROUP BY patient_id, wearable_date
"""
wdss_df = spark.sql(wdss_sql)
wdss_df.createOrReplaceTempView("wearable_daily_summary_silver")

(
    wdss_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_daily_summary_silver.csv")
)

# ==============================================
# TABLE: medication_administration_silver (mas)
# ==============================================
mas_sql = """
WITH joined AS (
  SELECT
    dab.patient_id AS patient_id,
    dab.administration_id AS administration_id,
    dab.drug_code AS medication_name,
    dab.drug_code AS drug_code,
    CAST(dab.dosage_mg AS DOUBLE) AS dosage_mg,
    CAST(dab.administration_date AS TIMESTAMP) AS administration_date,
    dab.administration_route AS administration_route
  FROM drug_administration_bronze dab
  INNER JOIN patient_enrollment_bronze peb
    ON dab.patient_id = peb.patient_id
  WHERE dab.patient_id IS NOT NULL
    AND dab.administration_id IS NOT NULL
),
dedup AS (
  SELECT
    patient_id,
    administration_id,
    medication_name,
    drug_code,
    dosage_mg,
    administration_date,
    administration_route,
    ROW_NUMBER() OVER (
      PARTITION BY administration_id
      ORDER BY administration_date DESC
    ) AS rn
  FROM joined
)
SELECT
  patient_id,
  administration_id,
  medication_name,
  drug_code,
  dosage_mg,
  administration_date,
  administration_route
FROM dedup
WHERE rn = 1
"""
mas_df = spark.sql(mas_sql)
mas_df.createOrReplaceTempView("medication_administration_silver")

(
    mas_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/medication_administration_silver.csv")
)

# ================================================
# TABLE: patient_therapy_episodes_silver (ptes)
# ================================================
ptes_sql = """
WITH ordered AS (
  SELECT
    mas.patient_id AS patient_id,
    mas.medication_name AS medication_name,
    mas.administration_date AS administration_date,
    LAG(mas.administration_date) OVER (
      PARTITION BY mas.patient_id, mas.medication_name
      ORDER BY mas.administration_date
    ) AS prev_administration_date
  FROM medication_administration_silver mas
),
marked AS (
  SELECT
    patient_id,
    medication_name,
    administration_date,
    CASE
      WHEN prev_administration_date IS NULL THEN 1
      WHEN DATEDIFF(administration_date, prev_administration_date) > 30 THEN 1
      ELSE 0
    END AS new_episode_flag
  FROM ordered
),
episode_ids AS (
  SELECT
    patient_id,
    medication_name,
    administration_date,
    SUM(new_episode_flag) OVER (
      PARTITION BY patient_id, medication_name
      ORDER BY administration_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS episode_id
  FROM marked
)
SELECT
  patient_id,
  medication_name,
  MIN(administration_date) AS treatment_start_date,
  MAX(administration_date) AS treatment_end_date
FROM episode_ids
GROUP BY patient_id, medication_name, episode_id
"""
ptes_df = spark.sql(ptes_sql)
ptes_df.createOrReplaceTempView("patient_therapy_episodes_silver")

(
    ptes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_therapy_episodes_silver.csv")
)

# =================================================
# TABLE: patient_lab_trend_inputs_silver (pltis)
# =================================================
pltis_sql = """
WITH filtered AS (
  SELECT
    lrs.patient_id AS patient_id,
    lrs.test_date AS test_date,
    lrs.test_type AS test_type,
    lrs.test_result_value AS test_result_value,
    lrs.result_unit AS result_unit,
    lrs.abnormal_flag AS abnormal_flag
  FROM lab_results_silver lrs
  WHERE lrs.test_type IN ('HBA1C', 'GLUCOSE')
    AND lrs.test_result_value IS NOT NULL
),
dedup AS (
  SELECT
    patient_id,
    test_date,
    test_type,
    test_result_value,
    result_unit,
    abnormal_flag,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, test_type, test_date
      ORDER BY CASE WHEN result_unit IS NULL THEN 1 ELSE 0 END ASC
    ) AS rn
  FROM filtered
)
SELECT
  patient_id,
  test_date,
  test_type,
  test_result_value,
  result_unit,
  abnormal_flag
FROM dedup
WHERE rn = 1
"""
pltis_df = spark.sql(pltis_sql)
pltis_df.createOrReplaceTempView("patient_lab_trend_inputs_silver")

(
    pltis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_lab_trend_inputs_silver.csv")
)

# =========================================
# TABLE: patient_risk_features_silver (prfs)
# =========================================
prfs_sql = """
SELECT
  pds.patient_id AS patient_id,
  MAX(DATE(pltis.test_date)) AS feature_as_of_date,
  CAST(FLOOR(DATEDIFF(CURRENT_DATE, pds.date_of_birth)/365.25) AS INT) AS age_years,
  pds.country AS country,
  MAX_BY(pltis.test_result_value, pltis.test_date) FILTER (WHERE pltis.test_type = 'GLUCOSE') AS latest_glucose_value,
  MAX_BY(pltis.test_result_value, pltis.test_date) FILTER (WHERE pltis.test_type = 'HBA1C') AS latest_hba1c_value,
  SUM(
    CASE
      WHEN pltis.test_type = 'GLUCOSE'
       AND pltis.abnormal_flag = 'Y'
       AND pltis.test_date >= DATEADD(day, -90, CURRENT_DATE)
      THEN 1 ELSE 0
    END
  ) AS abnormal_glucose_count_90d,
  SUM(
    CASE
      WHEN pltis.test_type = 'HBA1C'
       AND pltis.abnormal_flag = 'Y'
       AND pltis.test_date >= DATEADD(day, -90, CURRENT_DATE)
      THEN 1 ELSE 0
    END
  ) AS abnormal_hba1c_count_90d,
  AVG(wdss.glucose_avg) FILTER (WHERE wdss.wearable_date >= DATEADD(day, -7, CURRENT_DATE)) AS wearable_glucose_avg_7d,
  SUM(wdss.steps_total) FILTER (WHERE wdss.wearable_date >= DATEADD(day, -7, CURRENT_DATE)) AS steps_total_7d,
  SUM(wdss.sleep_hours_total) FILTER (WHERE wdss.wearable_date >= DATEADD(day, -7, CURRENT_DATE)) AS sleep_hours_total_7d,
  AVG(wdss.heart_rate_avg) FILTER (WHERE wdss.wearable_date >= DATEADD(day, -7, CURRENT_DATE)) AS heart_rate_avg_7d
FROM patient_demographics_silver pds
LEFT JOIN wearable_daily_summary_silver wdss
  ON pds.patient_id = wdss.patient_id
LEFT JOIN patient_lab_trend_inputs_silver pltis
  ON pds.patient_id = pltis.patient_id
GROUP BY
  pds.patient_id,
  pds.date_of_birth,
  pds.country
"""
prfs_df = spark.sql(prfs_sql)
prfs_df.createOrReplaceTempView("patient_risk_features_silver")

(
    prfs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_risk_features_silver.csv")
)

# ==================================================
# TABLE: integrated_patient_snapshot_silver (ipss)
# ==================================================
ipss_sql = """
WITH base AS (
  SELECT
    pds.patient_id AS patient_id,
    JSON_OBJECT(
      'patient_id', pds.patient_id,
      'trial_id', pds.trial_id,
      'site_id', pds.site_id,
      'patient_name', pds.patient_name,
      'gender', pds.gender,
      'date_of_birth', pds.date_of_birth,
      'country', pds.country,
      'enrollment_date', pds.enrollment_date,
      'consent_status', pds.consent_status,
      'source_system', pds.source_system
    ) AS demographics,
    JSON_OBJECT(
      'trial_id', pds.trial_id,
      'site_id', pds.site_id
    ) AS health_history,
    JSON_OBJECT(
      'wearable_glucose_avg_7d', prfs.wearable_glucose_avg_7d,
      'steps_total_7d', prfs.steps_total_7d,
      'sleep_hours_total_7d', prfs.sleep_hours_total_7d,
      'heart_rate_avg_7d', prfs.heart_rate_avg_7d
    ) AS wearable_data_summary,
    JSON_OBJECT(
      'latest_glucose_value', prfs.latest_glucose_value,
      'latest_hba1c_value', prfs.latest_hba1c_value,
      'abnormal_glucose_count_90d', prfs.abnormal_glucose_count_90d,
      'abnormal_hba1c_count_90d', prfs.abnormal_hba1c_count_90d
    ) AS combined_lab_results,
    prfs.feature_as_of_date AS snapshot_as_of_date
  FROM patient_demographics_silver pds
  LEFT JOIN patient_risk_features_silver prfs
    ON pds.patient_id = prfs.patient_id
),
dedup AS (
  SELECT
    patient_id,
    demographics,
    health_history,
    wearable_data_summary,
    combined_lab_results,
    snapshot_as_of_date,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id
      ORDER BY snapshot_as_of_date DESC
    ) AS rn
  FROM base
)
SELECT
  patient_id,
  demographics,
  health_history,
  wearable_data_summary,
  combined_lab_results,
  snapshot_as_of_date
FROM dedup
WHERE rn = 1
"""
ipss_df = spark.sql(ipss_sql)

(
    ipss_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/integrated_patient_snapshot_silver.csv")
)
