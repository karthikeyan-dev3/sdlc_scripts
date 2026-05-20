import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =====================================================================================
# Read Bronze Sources (S3) + Temp Views
# =====================================================================================

patient_enrollment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
patient_enrollment_bronze_df.createOrReplaceTempView("patient_enrollment_bronze")

clinical_visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visits_bronze.{FILE_FORMAT}/")
)
clinical_visits_bronze_df.createOrReplaceTempView("clinical_visits_bronze")

laboratory_tests_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/laboratory_tests_bronze.{FILE_FORMAT}/")
)
laboratory_tests_bronze_df.createOrReplaceTempView("laboratory_tests_bronze")

drug_administrations_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administrations_bronze.{FILE_FORMAT}/")
)
drug_administrations_bronze_df.createOrReplaceTempView("drug_administrations_bronze")

adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
adverse_events_bronze_df.createOrReplaceTempView("adverse_events_bronze")

wearable_device_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_device_data_bronze.{FILE_FORMAT}/")
)
wearable_device_data_bronze_df.createOrReplaceTempView("wearable_device_data_bronze")

# =====================================================================================
# silver.patient_enrollment_silver
# Columns: patient_id, enrollment_date
# =====================================================================================

patient_enrollment_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(peb.patient_id)) AS patient_id,
        CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date
      FROM patient_enrollment_bronze peb
    ),
    dedup AS (
      SELECT
        patient_id,
        enrollment_date,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id
          ORDER BY enrollment_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      enrollment_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollment_silver.csv")
)

patient_enrollment_silver_df.createOrReplaceTempView("patient_enrollment_silver")

# =====================================================================================
# silver.clinical_visits_silver
# Columns: visit_id, patient_id, clinical_visit_date
# =====================================================================================

clinical_visits_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        cvb.visit_id AS visit_id,
        UPPER(TRIM(cvb.patient_id)) AS patient_id,
        CAST(cvb.visit_date AS TIMESTAMP) AS clinical_visit_date
      FROM clinical_visits_bronze cvb
    ),
    dedup AS (
      SELECT
        visit_id,
        patient_id,
        clinical_visit_date,
        ROW_NUMBER() OVER (
          PARTITION BY visit_id
          ORDER BY clinical_visit_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      visit_id,
      patient_id,
      clinical_visit_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    clinical_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_visits_silver.csv")
)

clinical_visits_silver_df.createOrReplaceTempView("clinical_visits_silver")

# =====================================================================================
# silver.laboratory_tests_silver
# Columns: laboratory_test_id, patient_id, test_date
# =====================================================================================

laboratory_tests_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        ltb.lab_result_id AS laboratory_test_id,
        UPPER(TRIM(ltb.patient_id)) AS patient_id,
        CAST(ltb.test_date AS TIMESTAMP) AS test_date
      FROM laboratory_tests_bronze ltb
    ),
    dedup AS (
      SELECT
        laboratory_test_id,
        patient_id,
        test_date,
        ROW_NUMBER() OVER (
          PARTITION BY laboratory_test_id
          ORDER BY test_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      laboratory_test_id,
      patient_id,
      test_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    laboratory_tests_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/laboratory_tests_silver.csv")
)

laboratory_tests_silver_df.createOrReplaceTempView("laboratory_tests_silver")

# =====================================================================================
# silver.drug_administrations_silver
# Columns: drug_administration_id, patient_id, administration_date
# =====================================================================================

drug_administrations_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        dab.administration_id AS drug_administration_id,
        UPPER(TRIM(dab.patient_id)) AS patient_id,
        CAST(dab.administration_date AS TIMESTAMP) AS administration_date
      FROM drug_administrations_bronze dab
    ),
    dedup AS (
      SELECT
        drug_administration_id,
        patient_id,
        administration_date,
        ROW_NUMBER() OVER (
          PARTITION BY drug_administration_id
          ORDER BY administration_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      drug_administration_id,
      patient_id,
      administration_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    drug_administrations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_administrations_silver.csv")
)

drug_administrations_silver_df.createOrReplaceTempView("drug_administrations_silver")

# =====================================================================================
# silver.adverse_events_silver
# Columns: adverse_event_id, patient_id, event_start_date, event_end_date
# =====================================================================================

adverse_events_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        aeb.event_id AS adverse_event_id,
        UPPER(TRIM(aeb.patient_id)) AS patient_id,
        CAST(aeb.event_start_date AS TIMESTAMP) AS event_start_date,
        CAST(aeb.event_end_date AS TIMESTAMP) AS event_end_date
      FROM adverse_events_bronze aeb
    ),
    dedup AS (
      SELECT
        adverse_event_id,
        patient_id,
        event_start_date,
        event_end_date,
        ROW_NUMBER() OVER (
          PARTITION BY adverse_event_id
          ORDER BY event_start_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      adverse_event_id,
      patient_id,
      event_start_date,
      event_end_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_events_silver.csv")
)

adverse_events_silver_df.createOrReplaceTempView("adverse_events_silver")

# =====================================================================================
# silver.wearable_data_silver
# Columns: patient_id, wearable_device_id, data_timestamp, heart_rate, activity_level
# =====================================================================================

wearable_data_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(wdb.patient_id)) AS patient_id,
        wdb.device_record_id AS wearable_device_id,
        CAST(wdb.recorded_timestamp AS TIMESTAMP) AS data_timestamp,
        CAST(wdb.heart_rate AS DOUBLE) AS heart_rate,
        CAST(wdb.step_count AS INT) AS activity_level
      FROM wearable_device_data_bronze wdb
    ),
    dedup AS (
      SELECT
        patient_id,
        wearable_device_id,
        data_timestamp,
        heart_rate,
        activity_level,
        ROW_NUMBER() OVER (
          PARTITION BY wearable_device_id, data_timestamp
          ORDER BY data_timestamp DESC
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      wearable_device_id,
      data_timestamp,
      heart_rate,
      activity_level
    FROM dedup
    WHERE rn = 1
    """
)

(
    wearable_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_data_silver.csv")
)

wearable_data_silver_df.createOrReplaceTempView("wearable_data_silver")

# =====================================================================================
# silver.patient_data_unified_silver
# Columns: patient_id, enrollment_date, clinical_visit_date, laboratory_test_id,
#          drug_administration_id, adverse_event_id, wearable_device_data
# =====================================================================================

patient_data_unified_silver_df = spark.sql(
    """
    SELECT
      pes.patient_id AS patient_id,
      pes.enrollment_date AS enrollment_date,
      cvs.clinical_visit_date AS clinical_visit_date,
      lts.laboratory_test_id AS laboratory_test_id,
      das.drug_administration_id AS drug_administration_id,
      aes.adverse_event_id AS adverse_event_id,
      wds.wearable_device_id AS wearable_device_data
    FROM patient_enrollment_silver pes
    LEFT JOIN clinical_visits_silver cvs
      ON pes.patient_id = cvs.patient_id
    LEFT JOIN laboratory_tests_silver lts
      ON pes.patient_id = lts.patient_id
    LEFT JOIN drug_administrations_silver das
      ON pes.patient_id = das.patient_id
    LEFT JOIN adverse_events_silver aes
      ON pes.patient_id = aes.patient_id
    LEFT JOIN wearable_data_silver wds
      ON pes.patient_id = wds.patient_id
    """
)

(
    patient_data_unified_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_data_unified_silver.csv")
)

patient_data_unified_silver_df.createOrReplaceTempView("patient_data_unified_silver")

# =====================================================================================
# silver.clinical_dashboard_silver
# Columns: dashboard_id, patient_id, analysis_date, real_time_insights, decision_support_metrics
# =====================================================================================

clinical_dashboard_silver_df = spark.sql(
    """
    SELECT
      pdus.dashboard_id AS dashboard_id,
      pdus.patient_id AS patient_id,
      wds.data_timestamp AS analysis_date,
      pdus.real_time_insights AS real_time_insights,
      pdus.decision_support_metrics AS decision_support_metrics
    FROM patient_data_unified_silver pdus
    LEFT JOIN wearable_data_silver wds
      ON pdus.patient_id = wds.patient_id
    """
)

(
    clinical_dashboard_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_dashboard_silver.csv")
)

clinical_dashboard_silver_df.createOrReplaceTempView("clinical_dashboard_silver")

# =====================================================================================
# silver.safety_monitoring_silver
# Columns: patient_id, monitoring_date, safety_alert, operational_report_id
# =====================================================================================

safety_monitoring_silver_df = spark.sql(
    """
    SELECT
      aes.patient_id AS patient_id,
      aes.monitoring_date AS monitoring_date,
      aes.safety_alert AS safety_alert,
      aes.operational_report_id AS operational_report_id
    FROM adverse_events_silver aes
    LEFT JOIN drug_administrations_silver das
      ON aes.patient_id = das.patient_id
    """
)

(
    safety_monitoring_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/safety_monitoring_silver.csv")
)

safety_monitoring_silver_df.createOrReplaceTempView("safety_monitoring_silver")

# =====================================================================================
# silver.data_quality_silver
# Columns: data_quality_score, historical_consistency_check, compliance_check_status
# =====================================================================================

data_quality_silver_df = spark.sql(
    """
    SELECT
      pes.data_quality_score AS data_quality_score,
      pes.historical_consistency_check AS historical_consistency_check,
      pes.compliance_check_status AS compliance_check_status
    FROM patient_enrollment_silver pes
    LEFT JOIN clinical_visits_silver cvs
      ON pes.patient_id = cvs.patient_id
    LEFT JOIN laboratory_tests_silver lts
      ON pes.patient_id = lts.patient_id
    LEFT JOIN drug_administrations_silver das
      ON pes.patient_id = das.patient_id
    LEFT JOIN adverse_events_silver aes
      ON pes.patient_id = aes.patient_id
    LEFT JOIN wearable_data_silver wds
      ON pes.patient_id = wds.patient_id
    """
)

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_silver.csv")
)

data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

job.commit()
