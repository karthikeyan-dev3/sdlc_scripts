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

# =====================================================================================
# 1) READ SOURCE TABLES (BRONZE)
# =====================================================================================

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

lab_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
lab_results_bronze_df.createOrReplaceTempView("lab_results_bronze")

drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
drug_administration_bronze_df.createOrReplaceTempView("drug_administration_bronze")

adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
adverse_events_bronze_df.createOrReplaceTempView("adverse_events_bronze")

wearable_monitoring_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)
wearable_monitoring_bronze_df.createOrReplaceTempView("wearable_monitoring_bronze")

# =====================================================================================
# 2) TARGET: patient_enrollment_silver
# =====================================================================================

patient_enrollment_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(peb.patient_id)) AS patient_id,
        CAST(peb.enrollment_date AS timestamp) AS enrollment_date,
        peb.trial_id AS clinical_trial_id
      FROM patient_enrollment_bronze peb
      WHERE UPPER(TRIM(peb.patient_id)) IS NOT NULL
    ),
    ranked AS (
      SELECT
        patient_id,
        enrollment_date,
        clinical_trial_id,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, clinical_trial_id, enrollment_date
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      enrollment_date,
      clinical_trial_id
    FROM ranked
    WHERE rn = 1
    """
)
patient_enrollment_silver_df.createOrReplaceTempView("patient_enrollment_silver")

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollment_silver.csv")
)

# =====================================================================================
# 3) TARGET: clinical_visits_silver
# =====================================================================================

clinical_visits_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(cvb.patient_id)) AS patient_id,
        CAST(cvb.visit_date AS timestamp) AS visit_date,
        UPPER(TRIM(cvb.visit_type)) AS visit_type,
        cvb.trial_id AS clinical_trial_id,
        CAST(NULL AS string) AS doctor_id
      FROM clinical_visit_bronze cvb
    ),
    ranked AS (
      SELECT
        patient_id,
        visit_date,
        visit_type,
        clinical_trial_id,
        doctor_id,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, clinical_trial_id, visit_date, visit_type
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      visit_date,
      visit_type,
      clinical_trial_id,
      doctor_id
    FROM ranked
    WHERE rn = 1
    """
)
clinical_visits_silver_df.createOrReplaceTempView("clinical_visits_silver")

(
    clinical_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_visits_silver.csv")
)

# =====================================================================================
# 4) TARGET: lab_results_silver
# =====================================================================================

lab_results_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(lrb.patient_id)) AS patient_id,
        CAST(lrb.test_date AS timestamp) AS test_date,
        lrb.test_name AS test_type,
        lrb.test_result AS result_value,
        lrb.test_unit AS result_unit
      FROM lab_results_bronze lrb
      WHERE UPPER(TRIM(lrb.patient_id)) IS NOT NULL
        AND CAST(lrb.test_date AS timestamp) IS NOT NULL
    ),
    ranked AS (
      SELECT
        patient_id,
        test_date,
        test_type,
        result_value,
        result_unit,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, test_date, test_type, result_unit
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      test_date,
      test_type,
      result_value,
      result_unit
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

# =====================================================================================
# 5) TARGET: drug_administration_silver
# =====================================================================================

drug_administration_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(dab.patient_id)) AS patient_id,
        CAST(dab.administration_date AS timestamp) AS administration_date,
        dab.drug_code AS drug_name,
        dab.dosage_mg AS dosage,
        dab.administration_route AS route_of_administration
      FROM drug_administration_bronze dab
      WHERE UPPER(TRIM(dab.patient_id)) IS NOT NULL
        AND CAST(dab.administration_date AS timestamp) IS NOT NULL
    ),
    ranked AS (
      SELECT
        patient_id,
        administration_date,
        drug_name,
        dosage,
        route_of_administration,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, administration_date, drug_name, dosage, route_of_administration
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      administration_date,
      drug_name,
      dosage,
      route_of_administration
    FROM ranked
    WHERE rn = 1
    """
)
drug_administration_silver_df.createOrReplaceTempView("drug_administration_silver")

(
    drug_administration_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_administration_silver.csv")
)

# =====================================================================================
# 6) TARGET: adverse_events_silver
# =====================================================================================

adverse_events_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(aeb.patient_id)) AS patient_id,
        CAST(aeb.event_start_date AS timestamp) AS event_date,
        aeb.event_type AS event_description,
        UPPER(TRIM(aeb.severity)) AS severity,
        peb.trial_id AS clinical_trial_id
      FROM adverse_events_bronze aeb
      LEFT JOIN patient_enrollment_bronze peb
        ON aeb.patient_id = peb.patient_id
      WHERE UPPER(TRIM(aeb.patient_id)) IS NOT NULL
        AND CAST(aeb.event_start_date AS timestamp) IS NOT NULL
    ),
    ranked AS (
      SELECT
        patient_id,
        event_date,
        event_description,
        severity,
        clinical_trial_id,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, event_date, event_description, severity
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      event_date,
      event_description,
      severity,
      clinical_trial_id
    FROM ranked
    WHERE rn = 1
    """
)
adverse_events_silver_df.createOrReplaceTempView("adverse_events_silver")

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_events_silver.csv")
)

# =====================================================================================
# 7) TARGET: wearable_data_silver
# =====================================================================================

wearable_data_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(wmb.patient_id)) AS patient_id,
        CAST(wmb.recorded_timestamp AS timestamp) AS data_timestamp,
        wmb.heart_rate AS heart_rate,
        wmb.step_count AS steps_count,
        CAST(NULL AS double) AS calorie_burn
      FROM wearable_monitoring_bronze wmb
      WHERE UPPER(TRIM(wmb.patient_id)) IS NOT NULL
        AND CAST(wmb.recorded_timestamp AS timestamp) IS NOT NULL
    ),
    ranked AS (
      SELECT
        patient_id,
        data_timestamp,
        heart_rate,
        steps_count,
        calorie_burn,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id, data_timestamp
          ORDER BY patient_id
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      data_timestamp,
      heart_rate,
      steps_count,
      calorie_burn
    FROM ranked
    WHERE rn = 1
    """
)
wearable_data_silver_df.createOrReplaceTempView("wearable_data_silver")

(
    wearable_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_data_silver.csv")
)

# =====================================================================================
# 8) TARGET: patient_activity_summary_silver
# =====================================================================================

patient_activity_summary_silver_df = spark.sql(
    """
    SELECT
      pes.patient_id AS patient_id,
      COUNT(DISTINCT pes.clinical_trial_id) AS aggregate_clinical_trials,
      COUNT(cvs.visit_date) AS total_visits,
      MAX(lrs.test_date) AS last_lab_result_date,
      COUNT(aes.event_date) AS adverse_event_count
    FROM patient_enrollment_silver pes
    LEFT JOIN clinical_visits_silver cvs
      ON pes.patient_id = cvs.patient_id
    LEFT JOIN lab_results_silver lrs
      ON pes.patient_id = lrs.patient_id
    LEFT JOIN adverse_events_silver aes
      ON pes.patient_id = aes.patient_id
    GROUP BY
      pes.patient_id
    """
)

(
    patient_activity_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_activity_summary_silver.csv")
)

job.commit()
