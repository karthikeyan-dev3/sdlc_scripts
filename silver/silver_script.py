import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables (S3)
# -----------------------------
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
wdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_data_bronze.{FILE_FORMAT}/")
)
sal_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/system_audit_logs_raw.{FILE_FORMAT}/")
)
ura_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/iam_user_role_assignments_raw.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
peb_df.createOrReplaceTempView("peb")
cvb_df.createOrReplaceTempView("cvb")
lrb_df.createOrReplaceTempView("lrb")
wdb_df.createOrReplaceTempView("wdb")
sal_df.createOrReplaceTempView("sal")
ura_df.createOrReplaceTempView("ura")

# ============================================================
# Table: silver.patient_enrollment_silver
# ============================================================
patient_enrollment_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(peb.patient_id)) AS patient_id,
        UPPER(TRIM(peb.trial_id)) AS trial_id,
        UPPER(TRIM(peb.site_id)) AS site_id,
        CAST(peb.enrollment_date AS DATE) AS enrollment_date,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(peb.patient_id)), UPPER(TRIM(peb.trial_id))
          ORDER BY CAST(peb.enrollment_date AS DATE) DESC
        ) AS rn
      FROM peb
      WHERE
        UPPER(TRIM(peb.consent_status)) IN ('CONSENTED','YES','Y','TRUE','1')
        AND UPPER(TRIM(peb.patient_id)) IS NOT NULL
        AND UPPER(TRIM(peb.trial_id)) IS NOT NULL
        AND CAST(peb.enrollment_date AS DATE) IS NOT NULL
    )
    SELECT
      patient_id,
      trial_id,
      site_id,
      enrollment_date
    FROM base
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

patient_enrollment_silver_df.createOrReplaceTempView("pes")

# ============================================================
# Table: silver.clinical_visits_silver
# ============================================================
clinical_visits_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(cvb.patient_id)) AS patient_id,
        UPPER(TRIM(cvb.trial_id)) AS trial_id,
        CAST(cvb.visit_date AS DATE) AS visit_date,
        UPPER(TRIM(cvb.visit_type)) AS visit_type,
        CASE
          WHEN pes.patient_id IS NOT NULL
           AND pes.trial_id IS NOT NULL
           AND pes.enrollment_date <= CAST(cvb.visit_date AS DATE)
          THEN 'ADHERENT'
          ELSE 'NON_ADHERENT'
        END AS adherence_status,
        ROW_NUMBER() OVER (
          PARTITION BY
            UPPER(TRIM(cvb.patient_id)),
            UPPER(TRIM(cvb.trial_id)),
            CAST(cvb.visit_date AS DATE),
            UPPER(TRIM(cvb.visit_type))
          ORDER BY UPPER(TRIM(cvb.source_system)) DESC
        ) AS rn
      FROM cvb
      LEFT JOIN pes
        ON UPPER(TRIM(cvb.patient_id)) = pes.patient_id
       AND UPPER(TRIM(cvb.trial_id)) = pes.trial_id
      WHERE
        UPPER(TRIM(cvb.patient_id)) IS NOT NULL
        AND UPPER(TRIM(cvb.trial_id)) IS NOT NULL
        AND CAST(cvb.visit_date AS DATE) IS NOT NULL
    )
    SELECT
      patient_id,
      trial_id,
      visit_date,
      visit_type,
      adherence_status
    FROM base
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

# ============================================================
# Table: silver.lab_results_silver
# ============================================================
lab_results_silver_df = spark.sql(
    """
    WITH lab_base AS (
      SELECT
        UPPER(TRIM(lrb.patient_id)) AS patient_id,
        CAST(lrb.test_date AS DATE) AS result_date,
        UPPER(TRIM(lrb.test_name)) AS test_type,
        CAST(lrb.test_result AS DOUBLE) AS test_result,
        lrb.lab_result_id AS lab_result_id
      FROM lrb
      WHERE
        UPPER(TRIM(lrb.patient_id)) IS NOT NULL
        AND CAST(lrb.test_date AS DATE) IS NOT NULL
        AND UPPER(TRIM(lrb.test_name)) IS NOT NULL
    ),
    lab_with_trial AS (
      SELECT
        lb.patient_id,
        pe.trial_id AS trial_id,
        lb.result_date,
        lb.test_type,
        lb.test_result,
        lb.lab_result_id,
        ROW_NUMBER() OVER (
          PARTITION BY lb.patient_id, lb.result_date, lb.test_type
          ORDER BY pe.enrollment_date DESC, lb.lab_result_id DESC
        ) AS rn
      FROM lab_base lb
      LEFT JOIN pes pe
        ON lb.patient_id = pe.patient_id
       AND pe.enrollment_date <= lb.result_date
    )
    SELECT
      patient_id,
      trial_id,
      result_date,
      test_type,
      test_result
    FROM lab_with_trial
    WHERE rn = 1
    """
)

(
    lab_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_silver.csv")
)

# ============================================================
# Table: silver.wearable_data_silver
# ============================================================
wearable_data_silver_df = spark.sql(
    """
    WITH wearable_base AS (
      SELECT
        UPPER(TRIM(wdb.patient_id)) AS patient_id,
        UPPER(TRIM(wdb.device_record_id)) AS device_id,
        CAST(wdb.recorded_timestamp AS DATE) AS measurement_date,
        wdb.device_record_id AS device_record_id,
        wdb.glucose_level AS glucose_level,
        wdb.step_count AS step_count,
        wdb.sleep_hours AS sleep_hours,
        wdb.heart_rate AS heart_rate
      FROM wdb
      WHERE
        UPPER(TRIM(wdb.patient_id)) IS NOT NULL
        AND CAST(wdb.recorded_timestamp AS DATE) IS NOT NULL
    ),
    unpivoted AS (
      SELECT
        patient_id,
        device_id,
        measurement_date,
        'GLUCOSE_LEVEL' AS measurement_type,
        CAST(glucose_level AS DOUBLE) AS measurement_value,
        device_record_id
      FROM wearable_base
      WHERE glucose_level IS NOT NULL

      UNION ALL

      SELECT
        patient_id,
        device_id,
        measurement_date,
        'STEP_COUNT' AS measurement_type,
        CAST(step_count AS DOUBLE) AS measurement_value,
        device_record_id
      FROM wearable_base
      WHERE step_count IS NOT NULL

      UNION ALL

      SELECT
        patient_id,
        device_id,
        measurement_date,
        'SLEEP_HOURS' AS measurement_type,
        CAST(sleep_hours AS DOUBLE) AS measurement_value,
        device_record_id
      FROM wearable_base
      WHERE sleep_hours IS NOT NULL

      UNION ALL

      SELECT
        patient_id,
        device_id,
        measurement_date,
        'HEART_RATE' AS measurement_type,
        CAST(heart_rate AS DOUBLE) AS measurement_value,
        device_record_id
      FROM wearable_base
      WHERE heart_rate IS NOT NULL
    ),
    with_trial AS (
      SELECT
        u.patient_id,
        u.device_id,
        u.measurement_date,
        u.measurement_type,
        u.measurement_value,
        pe.trial_id AS trial_id,
        ROW_NUMBER() OVER (
          PARTITION BY u.patient_id, u.device_id, u.measurement_date, u.measurement_type
          ORDER BY pe.enrollment_date DESC, u.device_record_id DESC
        ) AS rn
      FROM unpivoted u
      LEFT JOIN pes pe
        ON u.patient_id = pe.patient_id
       AND pe.enrollment_date <= u.measurement_date
    )
    SELECT
      patient_id,
      device_id,
      measurement_date,
      measurement_type,
      measurement_value,
      trial_id
    FROM with_trial
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

# ============================================================
# Table: silver.data_audit_silver
# ============================================================
data_audit_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        sal.record_id AS record_id,
        sal.table_name AS table_name,
        UPPER(TRIM(sal.operation_type)) AS operation_type,
        CAST(sal.timestamp AS TIMESTAMP) AS timestamp,
        UPPER(TRIM(sal.user_id)) AS user_id,
        ROW_NUMBER() OVER (
          PARTITION BY sal.record_id
          ORDER BY CAST(sal.timestamp AS TIMESTAMP) DESC
        ) AS rn
      FROM sal
      WHERE
        sal.record_id IS NOT NULL
        AND sal.table_name IS NOT NULL
        AND sal.operation_type IS NOT NULL
        AND sal.timestamp IS NOT NULL
    )
    SELECT
      record_id,
      table_name,
      operation_type,
      timestamp,
      user_id
    FROM base
    WHERE rn = 1
    """
)

(
    data_audit_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_audit_silver.csv")
)

# ============================================================
# Table: silver.user_access_control_silver
# ============================================================
user_access_control_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(ura.user_id)) AS user_id,
        UPPER(TRIM(ura.role_id)) AS role_id,
        UPPER(TRIM(ura.access_level)) AS access_level,
        CAST(ura.assignment_date AS DATE) AS assignment_date,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(ura.user_id)), UPPER(TRIM(ura.role_id))
          ORDER BY CAST(ura.assignment_date AS DATE) DESC
        ) AS rn
      FROM ura
      WHERE
        UPPER(TRIM(ura.user_id)) IS NOT NULL
        AND UPPER(TRIM(ura.role_id)) IS NOT NULL
    )
    SELECT
      user_id,
      role_id,
      access_level,
      assignment_date
    FROM base
    WHERE rn = 1
    """
)

(
    user_access_control_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/user_access_control_silver.csv")
)

job.commit()
