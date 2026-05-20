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
# 1) READ SOURCE TABLES (BRONZE)
# ----------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
cvb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visits_bronze.{FILE_FORMAT}/")
)
lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
dab_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)

peb_df.createOrReplaceTempView("peb_raw")
cvb_df.createOrReplaceTempView("cvb_raw")
lrb_df.createOrReplaceTempView("lrb_raw")
dab_df.createOrReplaceTempView("dab_raw")
aeb_df.createOrReplaceTempView("aeb_raw")

# ----------------------------
# 2) DEDUP SOURCE SETS (ROW_NUMBER)
# ----------------------------
peb_dedup_sql = """
WITH ranked AS (
  SELECT
    patient_id,
    source_system,
    CAST(enrollment_date AS timestamp) AS enrollment_date,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id
      ORDER BY CAST(enrollment_date AS timestamp) DESC
    ) AS rn
  FROM peb_raw
)
SELECT
  patient_id,
  source_system,
  enrollment_date
FROM ranked
WHERE rn = 1
"""
spark.sql(peb_dedup_sql).createOrReplaceTempView("peb")

cvb_dedup_sql = """
WITH ranked AS (
  SELECT
    patient_id,
    visit_id,
    source_system,
    CAST(visit_date AS timestamp) AS visit_date,
    CAST(blood_pressure AS double) AS blood_pressure,
    CAST(heart_rate AS double) AS heart_rate,
    CAST(weight_kg AS double) AS weight_kg,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, visit_id
      ORDER BY CAST(visit_date AS timestamp) DESC
    ) AS rn
  FROM cvb_raw
)
SELECT
  patient_id,
  visit_id,
  source_system,
  visit_date,
  blood_pressure,
  heart_rate,
  weight_kg
FROM ranked
WHERE rn = 1
"""
spark.sql(cvb_dedup_sql).createOrReplaceTempView("cvb")

lrb_dedup_sql = """
WITH ranked AS (
  SELECT
    patient_id,
    lab_result_id,
    lab_name,
    CAST(test_date AS timestamp) AS test_date,
    test_name,
    CAST(test_result AS double) AS test_result,
    test_unit,
    abnormal_flag,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, lab_result_id
      ORDER BY CAST(test_date AS timestamp) DESC
    ) AS rn
  FROM lrb_raw
)
SELECT
  patient_id,
  lab_result_id,
  lab_name,
  test_date,
  test_name,
  test_result,
  test_unit,
  abnormal_flag
FROM ranked
WHERE rn = 1
"""
spark.sql(lrb_dedup_sql).createOrReplaceTempView("lrb")

dab_dedup_sql = """
WITH ranked AS (
  SELECT
    patient_id,
    administration_id,
    batch_number,
    CAST(administration_date AS timestamp) AS administration_date,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, administration_id
      ORDER BY CAST(administration_date AS timestamp) DESC
    ) AS rn
  FROM dab_raw
)
SELECT
  patient_id,
  administration_id,
  batch_number,
  administration_date
FROM ranked
WHERE rn = 1
"""
spark.sql(dab_dedup_sql).createOrReplaceTempView("dab")

aeb_dedup_sql = """
WITH ranked AS (
  SELECT
    patient_id,
    event_id,
    reported_by,
    CAST(event_start_date AS timestamp) AS event_start_date,
    severity,
    CAST(hospitalization_required AS boolean) AS hospitalization_required,
    CAST(related_to_drug AS boolean) AS related_to_drug,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, event_id
      ORDER BY CAST(event_start_date AS timestamp) DESC
    ) AS rn
  FROM aeb_raw
)
SELECT
  patient_id,
  event_id,
  reported_by,
  event_start_date,
  severity,
  hospitalization_required,
  related_to_drug
FROM ranked
WHERE rn = 1
"""
spark.sql(aeb_dedup_sql).createOrReplaceTempView("aeb")

# ============================================================
# TARGET TABLE 1: silver.patient_analytics_silver
# ============================================================
patient_analytics_sql = """
SELECT
  peb.patient_id AS patient_id,
  CAST(peb.enrollment_date AS timestamp) AS enrollment_date,
  CAST(cvb.visit_date AS timestamp) AS clinical_visit_date,
  CONCAT(
    COALESCE(TRIM(lrb.test_name), ''),
    ':',
    COALESCE(CAST(lrb.test_result AS string), ''),
    ' ',
    COALESCE(TRIM(lrb.test_unit), '')
  ) AS lab_result,
  CASE
    WHEN CAST(lrb.test_result AS double) IS NOT NULL AND CAST(lrb.test_result AS double) <= 0 THEN 'IMPROVING'
    WHEN CAST(lrb.test_result AS double) IS NOT NULL AND CAST(lrb.test_result AS double) > 0 THEN 'NOT_IMPROVING'
    WHEN cvb.blood_pressure IS NOT NULL OR cvb.heart_rate IS NOT NULL OR cvb.weight_kg IS NOT NULL THEN 'VITALS_PRESENT'
    ELSE NULL
  END AS treatment_effectiveness,
  CASE
    WHEN UPPER(TRIM(COALESCE(lrb.abnormal_flag, ''))) IN ('Y','YES','TRUE','T','1','ABNORMAL') THEN 'ABNORMAL_TREND'
    WHEN cvb.blood_pressure IS NOT NULL OR cvb.heart_rate IS NOT NULL OR cvb.weight_kg IS NOT NULL THEN 'VITALS_TREND'
    ELSE NULL
  END AS health_progression,
  CASE
    WHEN UPPER(TRIM(COALESCE(aeb.severity, ''))) IN ('SEVERE','HIGH') THEN 'HIGH'
    WHEN COALESCE(aeb.hospitalization_required, false) = true THEN 'HIGH'
    WHEN COALESCE(aeb.related_to_drug, false) = true THEN 'MEDIUM'
    WHEN UPPER(TRIM(COALESCE(lrb.abnormal_flag, ''))) IN ('Y','YES','TRUE','T','1','ABNORMAL') THEN 'MEDIUM'
    ELSE NULL
  END AS safety_risk,
  SHA2(
    CONCAT(
      COALESCE(TRIM(peb.source_system), ''),
      '|',
      UPPER(TRIM(COALESCE(peb.patient_id, '')))
    ),
    256
  ) AS standardized_patient_identifier
FROM peb
LEFT JOIN cvb
  ON peb.patient_id = cvb.patient_id
LEFT JOIN lrb
  ON peb.patient_id = lrb.patient_id
LEFT JOIN dab
  ON peb.patient_id = dab.patient_id
LEFT JOIN aeb
  ON peb.patient_id = aeb.patient_id
"""
pas_df = spark.sql(patient_analytics_sql)
pas_df.createOrReplaceTempView("pas")

patient_analytics_out = (
    pas_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .option("emptyValue", "")
)
patient_analytics_out.save(f"{TARGET_PATH}/patient_analytics_silver.csv")

# ============================================================
# TARGET TABLE 2: silver.dashboard_reporting_silver
# ============================================================
dashboard_reporting_sql = """
SELECT
  SHA2(
    CONCAT(
      COALESCE(TRIM(pas.standardized_patient_identifier), ''),
      '|',
      COALESCE(
        CAST(pas.clinical_visit_date AS string),
        CAST(pas.enrollment_date AS string)
      )
    ),
    256
  ) AS dashboard_id,
  pas.patient_id AS user_id,
  COALESCE(CAST(pas.clinical_visit_date AS timestamp), CAST(pas.enrollment_date AS timestamp)) AS access_date,
  CONCAT(
    COALESCE(TRIM(pas.treatment_effectiveness), ''),
    '|',
    COALESCE(TRIM(pas.health_progression), ''),
    '|',
    COALESCE(TRIM(pas.safety_risk), '')
  ) AS decision_support
FROM pas
"""
drs_df = spark.sql(dashboard_reporting_sql)
drs_df.createOrReplaceTempView("drs")

dashboard_reporting_out = (
    drs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .option("emptyValue", "")
)
dashboard_reporting_out.save(f"{TARGET_PATH}/dashboard_reporting_silver.csv")

# ============================================================
# TARGET TABLE 3: silver.data_governance_silver
# ============================================================
data_governance_sql = """
WITH peb_metrics AS (
  SELECT
    CAST(peb.source_system AS string) AS data_source,
    CAST(
      CASE
        WHEN TRIM(COALESCE(peb.patient_id, '')) <> '' AND peb.enrollment_date IS NOT NULL THEN 1.0
        WHEN TRIM(COALESCE(peb.patient_id, '')) <> '' OR peb.enrollment_date IS NOT NULL THEN 0.5
        ELSE 0.0
      END AS double
    ) AS quality_score,
    CAST(
      CONCAT(
        'peb_patient_id_present:', CASE WHEN TRIM(COALESCE(peb.patient_id, '')) <> '' THEN '1' ELSE '0' END,
        ',peb_enrollment_date_present:', CASE WHEN peb.enrollment_date IS NOT NULL THEN '1' ELSE '0' END,
        ',cvb_visit_id_present:0',
        ',cvb_visit_date_present:0',
        ',lrb_lab_result_id_present:0',
        ',lrb_test_date_present:0',
        ',dab_administration_id_present:0',
        ',dab_administration_date_present:0',
        ',aeb_event_id_present:0',
        ',aeb_event_start_date_present:0'
      ) AS string
    ) AS validation_checks,
    CURRENT_TIMESTAMP() AS compliance_audit_date
  FROM peb
),
cvb_metrics AS (
  SELECT
    CAST(cvb.source_system AS string) AS data_source,
    CAST(
      CASE
        WHEN TRIM(COALESCE(cvb.patient_id, '')) <> '' AND TRIM(COALESCE(cvb.visit_id, '')) <> '' AND cvb.visit_date IS NOT NULL THEN 1.0
        WHEN (TRIM(COALESCE(cvb.patient_id, '')) <> '' AND TRIM(COALESCE(cvb.visit_id, '')) <> '')
          OR (TRIM(COALESCE(cvb.patient_id, '')) <> '' AND cvb.visit_date IS NOT NULL)
          OR (TRIM(COALESCE(cvb.visit_id, '')) <> '' AND cvb.visit_date IS NOT NULL) THEN 0.5
        ELSE 0.0
      END AS double
    ) AS quality_score,
    CAST(
      CONCAT(
        'peb_patient_id_present:0',
        ',peb_enrollment_date_present:0',
        ',cvb_visit_id_present:', CASE WHEN TRIM(COALESCE(cvb.visit_id, '')) <> '' THEN '1' ELSE '0' END,
        ',cvb_visit_date_present:', CASE WHEN cvb.visit_date IS NOT NULL THEN '1' ELSE '0' END,
        ',lrb_lab_result_id_present:0',
        ',lrb_test_date_present:0',
        ',dab_administration_id_present:0',
        ',dab_administration_date_present:0',
        ',aeb_event_id_present:0',
        ',aeb_event_start_date_present:0'
      ) AS string
    ) AS validation_checks,
    CURRENT_TIMESTAMP() AS compliance_audit_date
  FROM cvb
),
lrb_metrics AS (
  SELECT
    CAST(lrb.lab_name AS string) AS data_source,
    CAST(
      CASE
        WHEN TRIM(COALESCE(lrb.patient_id, '')) <> '' AND TRIM(COALESCE(lrb.lab_result_id, '')) <> '' AND lrb.test_date IS NOT NULL THEN 1.0
        WHEN (TRIM(COALESCE(lrb.patient_id, '')) <> '' AND TRIM(COALESCE(lrb.lab_result_id, '')) <> '')
          OR (TRIM(COALESCE(lrb.patient_id, '')) <> '' AND lrb.test_date IS NOT NULL)
          OR (TRIM(COALESCE(lrb.lab_result_id, '')) <> '' AND lrb.test_date IS NOT NULL) THEN 0.5
        ELSE 0.0
      END AS double
    ) AS quality_score,
    CAST(
      CONCAT(
        'peb_patient_id_present:0',
        ',peb_enrollment_date_present:0',
        ',cvb_visit_id_present:0',
        ',cvb_visit_date_present:0',
        ',lrb_lab_result_id_present:', CASE WHEN TRIM(COALESCE(lrb.lab_result_id, '')) <> '' THEN '1' ELSE '0' END,
        ',lrb_test_date_present:', CASE WHEN lrb.test_date IS NOT NULL THEN '1' ELSE '0' END,
        ',dab_administration_id_present:0',
        ',dab_administration_date_present:0',
        ',aeb_event_id_present:0',
        ',aeb_event_start_date_present:0'
      ) AS string
    ) AS validation_checks,
    CURRENT_TIMESTAMP() AS compliance_audit_date
  FROM lrb
),
dab_metrics AS (
  SELECT
    CAST(dab.batch_number AS string) AS data_source,
    CAST(
      CASE
        WHEN TRIM(COALESCE(dab.patient_id, '')) <> '' AND TRIM(COALESCE(dab.administration_id, '')) <> '' AND dab.administration_date IS NOT NULL THEN 1.0
        WHEN (TRIM(COALESCE(dab.patient_id, '')) <> '' AND TRIM(COALESCE(dab.administration_id, '')) <> '')
          OR (TRIM(COALESCE(dab.patient_id, '')) <> '' AND dab.administration_date IS NOT NULL)
          OR (TRIM(COALESCE(dab.administration_id, '')) <> '' AND dab.administration_date IS NOT NULL) THEN 0.5
        ELSE 0.0
      END AS double
    ) AS quality_score,
    CAST(
      CONCAT(
        'peb_patient_id_present:0',
        ',peb_enrollment_date_present:0',
        ',cvb_visit_id_present:0',
        ',cvb_visit_date_present:0',
        ',lrb_lab_result_id_present:0',
        ',lrb_test_date_present:0',
        ',dab_administration_id_present:', CASE WHEN TRIM(COALESCE(dab.administration_id, '')) <> '' THEN '1' ELSE '0' END,
        ',dab_administration_date_present:', CASE WHEN dab.administration_date IS NOT NULL THEN '1' ELSE '0' END,
        ',aeb_event_id_present:0',
        ',aeb_event_start_date_present:0'
      ) AS string
    ) AS validation_checks,
    CURRENT_TIMESTAMP() AS compliance_audit_date
  FROM dab
),
aeb_metrics AS (
  SELECT
    CAST(aeb.reported_by AS string) AS data_source,
    CAST(
      CASE
        WHEN TRIM(COALESCE(aeb.patient_id, '')) <> '' AND TRIM(COALESCE(aeb.event_id, '')) <> '' AND aeb.event_start_date IS NOT NULL THEN 1.0
        WHEN (TRIM(COALESCE(aeb.patient_id, '')) <> '' AND TRIM(COALESCE(aeb.event_id, '')) <> '')
          OR (TRIM(COALESCE(aeb.patient_id, '')) <> '' AND aeb.event_start_date IS NOT NULL)
          OR (TRIM(COALESCE(aeb.event_id, '')) <> '' AND aeb.event_start_date IS NOT NULL) THEN 0.5
        ELSE 0.0
      END AS double
    ) AS quality_score,
    CAST(
      CONCAT(
        'peb_patient_id_present:0',
        ',peb_enrollment_date_present:0',
        ',cvb_visit_id_present:0',
        ',cvb_visit_date_present:0',
        ',lrb_lab_result_id_present:0',
        ',lrb_test_date_present:0',
        ',dab_administration_id_present:0',
        ',dab_administration_date_present:0',
        ',aeb_event_id_present:', CASE WHEN TRIM(COALESCE(aeb.event_id, '')) <> '' THEN '1' ELSE '0' END,
        ',aeb_event_start_date_present:', CASE WHEN aeb.event_start_date IS NOT NULL THEN '1' ELSE '0' END
      ) AS string
    ) AS validation_checks,
    CURRENT_TIMESTAMP() AS compliance_audit_date
  FROM aeb
)
SELECT
  data_source,
  quality_score,
  validation_checks,
  compliance_audit_date
FROM peb_metrics
UNION ALL
SELECT
  data_source,
  quality_score,
  validation_checks,
  compliance_audit_date
FROM cvb_metrics
UNION ALL
SELECT
  data_source,
  quality_score,
  validation_checks,
  compliance_audit_date
FROM lrb_metrics
UNION ALL
SELECT
  data_source,
  quality_score,
  validation_checks,
  compliance_audit_date
FROM dab_metrics
UNION ALL
SELECT
  data_source,
  quality_score,
  validation_checks,
  compliance_audit_date
FROM aeb_metrics
"""
dgs_df = spark.sql(data_governance_sql)
dgs_df.createOrReplaceTempView("dgs")

data_governance_out = (
    dgs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .option("emptyValue", "")
)
data_governance_out.save(f"{TARGET_PATH}/data_governance_silver.csv")

job.commit()
