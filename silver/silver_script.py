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

spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------
# 1) Read source tables (Bronze)
# -----------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
cvb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
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
wmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
peb_df.createOrReplaceTempView("peb")
cvb_df.createOrReplaceTempView("cvb")
lrb_df.createOrReplaceTempView("lrb")
dab_df.createOrReplaceTempView("dab")
aeb_df.createOrReplaceTempView("aeb")
wmb_df.createOrReplaceTempView("wmb")

# ============================================================
# TABLE: silver.patient_identity_silver
# ============================================================
patient_identity_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(peb.patient_id)) AS patient_id,
        TRIM(peb.source_system)     AS healthcare_system,
        TRIM(peb.trial_id)          AS trial_id,
        TRIM(peb.site_id)           AS site_id,
        CAST(peb.enrollment_date AS timestamp) AS enrollment_date,
        TRIM(peb.consent_status)    AS consent_status
      FROM peb
      WHERE peb.patient_id IS NOT NULL AND TRIM(peb.patient_id) <> ''
    ),
    dedup AS (
      SELECT
        patient_id,
        healthcare_system,
        trial_id,
        site_id,
        enrollment_date,
        consent_status,
        ROW_NUMBER() OVER (
          PARTITION BY patient_id
          ORDER BY enrollment_date DESC
        ) AS rn
      FROM base
    )
    SELECT
      patient_id,
      healthcare_system,
      trial_id,
      site_id,
      enrollment_date,
      consent_status
    FROM dedup
    WHERE rn = 1
    """
)

patient_identity_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_identity_silver.csv"
)

patient_identity_silver_df.createOrReplaceTempView("patient_identity_silver")

# ============================================================
# TABLE: silver.patient_clinical_events_silver
# ============================================================
patient_clinical_events_silver_df = spark.sql(
    """
    WITH visit_base AS (
      SELECT
        UPPER(TRIM(cvb.patient_id)) AS patient_id,
        TRIM(cvb.source_system)     AS healthcare_system,
        CAST(cvb.visit_date AS timestamp) AS record_timestamp,
        'VISIT' AS event_type,
        TRIM(cvb.visit_id) AS visit_id,
        TRIM(cvb.visit_type) AS visit_type,
        CAST(cvb.blood_pressure AS double) AS blood_pressure,
        CAST(cvb.heart_rate AS double) AS heart_rate,
        CAST(cvb.weight_kg AS double) AS weight_kg,
        TRIM(cvb.physician_notes) AS physician_notes,
        CAST(NULL AS string) AS lab_result_id,
        CAST(NULL AS string) AS test_name,
        CAST(NULL AS double) AS test_result,
        CAST(NULL AS string) AS test_unit,
        CAST(NULL AS string) AS reference_range,
        CAST(NULL AS string) AS abnormal_flag,
        CAST(NULL AS string) AS lab_name,
        CAST(NULL AS string) AS administration_id,
        CAST(NULL AS string) AS drug_code,
        CAST(NULL AS float)  AS dosage_mg,
        CAST(NULL AS string) AS administration_route,
        CAST(NULL AS string) AS administered_by,
        CAST(NULL AS string) AS batch_number,
        CAST(NULL AS string) AS event_id,
        CAST(NULL AS string) AS severity,
        CAST(NULL AS timestamp) AS event_end_date,
        CAST(NULL AS string) AS outcome,
        CAST(NULL AS boolean) AS related_to_drug,
        CAST(NULL AS boolean) AS hospitalization_required,
        CAST(NULL AS string) AS reported_by,
        CAST(NULL AS string) AS device_record_id,
        CAST(NULL AS string) AS device_type,
        CAST(NULL AS double) AS glucose_level,
        CAST(NULL AS int)    AS step_count,
        CAST(NULL AS double) AS sleep_hours,
        CAST(NULL AS string) AS battery_status
      FROM cvb
      WHERE cvb.patient_id IS NOT NULL AND TRIM(cvb.patient_id) <> ''
        AND cvb.visit_id IS NOT NULL AND TRIM(cvb.visit_id) <> ''
        AND cvb.visit_date IS NOT NULL
    ),
    visit_dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY visit_id ORDER BY record_timestamp DESC) AS rn
      FROM visit_base
    ),
    lab_base AS (
      SELECT
        UPPER(TRIM(lrb.patient_id)) AS patient_id,
        CAST(NULL AS string)        AS healthcare_system,
        CAST(lrb.test_date AS timestamp) AS record_timestamp,
        'LAB' AS event_type,
        CAST(NULL AS string) AS visit_id,
        CAST(NULL AS string) AS visit_type,
        CAST(NULL AS double) AS blood_pressure,
        CAST(NULL AS double) AS heart_rate,
        CAST(NULL AS double) AS weight_kg,
        CAST(NULL AS string) AS physician_notes,
        TRIM(lrb.lab_result_id) AS lab_result_id,
        TRIM(lrb.test_name) AS test_name,
        CAST(lrb.test_result AS double) AS test_result,
        TRIM(lrb.test_unit) AS test_unit,
        TRIM(lrb.reference_range) AS reference_range,
        TRIM(lrb.abnormal_flag) AS abnormal_flag,
        TRIM(lrb.lab_name) AS lab_name,
        CAST(NULL AS string) AS administration_id,
        CAST(NULL AS string) AS drug_code,
        CAST(NULL AS float)  AS dosage_mg,
        CAST(NULL AS string) AS administration_route,
        CAST(NULL AS string) AS administered_by,
        CAST(NULL AS string) AS batch_number,
        CAST(NULL AS string) AS event_id,
        CAST(NULL AS string) AS severity,
        CAST(NULL AS timestamp) AS event_end_date,
        CAST(NULL AS string) AS outcome,
        CAST(NULL AS boolean) AS related_to_drug,
        CAST(NULL AS boolean) AS hospitalization_required,
        CAST(NULL AS string) AS reported_by,
        CAST(NULL AS string) AS device_record_id,
        CAST(NULL AS string) AS device_type,
        CAST(NULL AS double) AS glucose_level,
        CAST(NULL AS int)    AS step_count,
        CAST(NULL AS double) AS sleep_hours,
        CAST(NULL AS string) AS battery_status
      FROM lrb
      WHERE lrb.patient_id IS NOT NULL AND TRIM(lrb.patient_id) <> ''
        AND lrb.lab_result_id IS NOT NULL AND TRIM(lrb.lab_result_id) <> ''
        AND lrb.test_date IS NOT NULL
    ),
    lab_dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY lab_result_id ORDER BY record_timestamp DESC) AS rn
      FROM lab_base
    ),
    drug_base AS (
      SELECT
        UPPER(TRIM(dab.patient_id)) AS patient_id,
        CAST(NULL AS string)        AS healthcare_system,
        CAST(dab.administration_date AS timestamp) AS record_timestamp,
        'DRUG' AS event_type,
        CAST(NULL AS string) AS visit_id,
        CAST(NULL AS string) AS visit_type,
        CAST(NULL AS double) AS blood_pressure,
        CAST(NULL AS double) AS heart_rate,
        CAST(NULL AS double) AS weight_kg,
        CAST(NULL AS string) AS physician_notes,
        CAST(NULL AS string) AS lab_result_id,
        CAST(NULL AS string) AS test_name,
        CAST(NULL AS double) AS test_result,
        CAST(NULL AS string) AS test_unit,
        CAST(NULL AS string) AS reference_range,
        CAST(NULL AS string) AS abnormal_flag,
        CAST(NULL AS string) AS lab_name,
        TRIM(dab.administration_id) AS administration_id,
        TRIM(dab.drug_code) AS drug_code,
        CAST(dab.dosage_mg AS float) AS dosage_mg,
        TRIM(dab.administration_route) AS administration_route,
        TRIM(dab.administered_by) AS administered_by,
        TRIM(dab.batch_number) AS batch_number,
        CAST(NULL AS string) AS event_id,
        CAST(NULL AS string) AS severity,
        CAST(NULL AS timestamp) AS event_end_date,
        CAST(NULL AS string) AS outcome,
        CAST(NULL AS boolean) AS related_to_drug,
        CAST(NULL AS boolean) AS hospitalization_required,
        CAST(NULL AS string) AS reported_by,
        CAST(NULL AS string) AS device_record_id,
        CAST(NULL AS string) AS device_type,
        CAST(NULL AS double) AS glucose_level,
        CAST(NULL AS int)    AS step_count,
        CAST(NULL AS double) AS sleep_hours,
        CAST(NULL AS string) AS battery_status
      FROM dab
      WHERE dab.patient_id IS NOT NULL AND TRIM(dab.patient_id) <> ''
        AND dab.administration_id IS NOT NULL AND TRIM(dab.administration_id) <> ''
        AND dab.administration_date IS NOT NULL
    ),
    drug_dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY administration_id ORDER BY record_timestamp DESC) AS rn
      FROM drug_base
    ),
    adverse_base AS (
      SELECT
        UPPER(TRIM(aeb.patient_id)) AS patient_id,
        CAST(NULL AS string)        AS healthcare_system,
        CAST(aeb.event_start_date AS timestamp) AS record_timestamp,
        'ADVERSE_EVENT' AS event_type,
        CAST(NULL AS string) AS visit_id,
        CAST(NULL AS string) AS visit_type,
        CAST(NULL AS double) AS blood_pressure,
        CAST(NULL AS double) AS heart_rate,
        CAST(NULL AS double) AS weight_kg,
        CAST(NULL AS string) AS physician_notes,
        CAST(NULL AS string) AS lab_result_id,
        CAST(NULL AS string) AS test_name,
        CAST(NULL AS double) AS test_result,
        CAST(NULL AS string) AS test_unit,
        CAST(NULL AS string) AS reference_range,
        CAST(NULL AS string) AS abnormal_flag,
        CAST(NULL AS string) AS lab_name,
        CAST(NULL AS string) AS administration_id,
        CAST(NULL AS string) AS drug_code,
        CAST(NULL AS float)  AS dosage_mg,
        CAST(NULL AS string) AS administration_route,
        CAST(NULL AS string) AS administered_by,
        CAST(NULL AS string) AS batch_number,
        TRIM(aeb.event_id) AS event_id,
        TRIM(aeb.severity) AS severity,
        CAST(aeb.event_end_date AS timestamp) AS event_end_date,
        TRIM(aeb.outcome) AS outcome,
        CAST(aeb.related_to_drug AS boolean) AS related_to_drug,
        CAST(aeb.hospitalization_required AS boolean) AS hospitalization_required,
        TRIM(aeb.reported_by) AS reported_by,
        CAST(NULL AS string) AS device_record_id,
        CAST(NULL AS string) AS device_type,
        CAST(NULL AS double) AS glucose_level,
        CAST(NULL AS int)    AS step_count,
        CAST(NULL AS double) AS sleep_hours,
        CAST(NULL AS string) AS battery_status
      FROM aeb
      WHERE aeb.patient_id IS NOT NULL AND TRIM(aeb.patient_id) <> ''
        AND aeb.event_id IS NOT NULL AND TRIM(aeb.event_id) <> ''
        AND aeb.event_start_date IS NOT NULL
    ),
    adverse_dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY record_timestamp DESC) AS rn
      FROM adverse_base
    ),
    wearable_base AS (
      SELECT
        UPPER(TRIM(wmb.patient_id)) AS patient_id,
        CAST(NULL AS string)        AS healthcare_system,
        CAST(wmb.recorded_timestamp AS timestamp) AS record_timestamp,
        'WEARABLE' AS event_type,
        CAST(NULL AS string) AS visit_id,
        CAST(NULL AS string) AS visit_type,
        CAST(NULL AS double) AS blood_pressure,
        CAST(NULL AS double) AS heart_rate,
        CAST(NULL AS double) AS weight_kg,
        CAST(NULL AS string) AS physician_notes,
        CAST(NULL AS string) AS lab_result_id,
        CAST(NULL AS string) AS test_name,
        CAST(NULL AS double) AS test_result,
        CAST(NULL AS string) AS test_unit,
        CAST(NULL AS string) AS reference_range,
        CAST(NULL AS string) AS abnormal_flag,
        CAST(NULL AS string) AS lab_name,
        CAST(NULL AS string) AS administration_id,
        CAST(NULL AS string) AS drug_code,
        CAST(NULL AS float)  AS dosage_mg,
        CAST(NULL AS string) AS administration_route,
        CAST(NULL AS string) AS administered_by,
        CAST(NULL AS string) AS batch_number,
        CAST(NULL AS string) AS event_id,
        CAST(NULL AS string) AS severity,
        CAST(NULL AS timestamp) AS event_end_date,
        CAST(NULL AS string) AS outcome,
        CAST(NULL AS boolean) AS related_to_drug,
        CAST(NULL AS boolean) AS hospitalization_required,
        CAST(NULL AS string) AS reported_by,
        TRIM(wmb.device_record_id) AS device_record_id,
        TRIM(wmb.device_type) AS device_type,
        CAST(wmb.glucose_level AS double) AS glucose_level,
        CAST(wmb.step_count AS int) AS step_count,
        CAST(wmb.sleep_hours AS double) AS sleep_hours,
        TRIM(wmb.battery_status) AS battery_status
      FROM wmb
      WHERE wmb.patient_id IS NOT NULL AND TRIM(wmb.patient_id) <> ''
        AND wmb.device_record_id IS NOT NULL AND TRIM(wmb.device_record_id) <> ''
        AND wmb.recorded_timestamp IS NOT NULL
    ),
    wearable_dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY device_record_id ORDER BY record_timestamp DESC) AS rn
      FROM wearable_base
    ),
    unioned AS (
      SELECT * FROM visit_dedup WHERE rn = 1
      UNION ALL
      SELECT * FROM lab_dedup WHERE rn = 1
      UNION ALL
      SELECT * FROM drug_dedup WHERE rn = 1
      UNION ALL
      SELECT * FROM adverse_dedup WHERE rn = 1
      UNION ALL
      SELECT * FROM wearable_dedup WHERE rn = 1
    )
    SELECT
      patient_id,
      healthcare_system,
      record_timestamp,
      event_type,
      visit_id,
      visit_type,
      blood_pressure,
      heart_rate,
      weight_kg,
      physician_notes,
      lab_result_id,
      test_name,
      test_result,
      test_unit,
      reference_range,
      abnormal_flag,
      lab_name,
      administration_id,
      drug_code,
      dosage_mg,
      administration_route,
      administered_by,
      batch_number,
      event_id,
      severity,
      event_end_date,
      outcome,
      related_to_drug,
      hospitalization_required,
      reported_by,
      device_record_id,
      device_type,
      glucose_level,
      step_count,
      sleep_hours,
      battery_status
    FROM unioned
    WHERE patient_id IS NOT NULL AND TRIM(patient_id) <> ''
      AND record_timestamp IS NOT NULL
    """
)

patient_clinical_events_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_clinical_events_silver.csv"
)

patient_clinical_events_silver_df.createOrReplaceTempView("patient_clinical_events_silver")

# ============================================================
# TABLE: silver.patient_dashboard_metrics_silver
# (UDT columns only: patient_id, metric_as_of_timestamp)
# ============================================================
patient_dashboard_metrics_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        pces.patient_id AS patient_id,
        CAST(pces.record_timestamp AS timestamp) AS metric_as_of_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY pces.patient_id, pces.record_timestamp
          ORDER BY pces.record_timestamp DESC
        ) AS rn
      FROM patient_clinical_events_silver pces
      LEFT JOIN patient_identity_silver pis
        ON pces.patient_id = pis.patient_id
      WHERE pces.patient_id IS NOT NULL AND TRIM(pces.patient_id) <> ''
        AND pces.record_timestamp IS NOT NULL
    )
    SELECT
      patient_id,
      metric_as_of_timestamp
    FROM base
    WHERE rn = 1
    """
)

patient_dashboard_metrics_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_dashboard_metrics_silver.csv"
)

patient_dashboard_metrics_silver_df.createOrReplaceTempView("patient_dashboard_metrics_silver")

# ============================================================
# TABLE: silver.data_process_monitor_silver
# (UDT columns only: data_updated_timestamp)
# ============================================================
data_process_monitor_silver_df = spark.sql(
    """
    WITH unioned AS (
      SELECT CAST(peb.enrollment_date AS timestamp) AS data_updated_timestamp
      FROM peb
      WHERE peb.enrollment_date IS NOT NULL
      UNION ALL
      SELECT CAST(cvb.visit_date AS timestamp) AS data_updated_timestamp
      FROM cvb
      WHERE cvb.visit_date IS NOT NULL
      UNION ALL
      SELECT CAST(lrb.test_date AS timestamp) AS data_updated_timestamp
      FROM lrb
      WHERE lrb.test_date IS NOT NULL
      UNION ALL
      SELECT CAST(dab.administration_date AS timestamp) AS data_updated_timestamp
      FROM dab
      WHERE dab.administration_date IS NOT NULL
      UNION ALL
      SELECT CAST(aeb.event_start_date AS timestamp) AS data_updated_timestamp
      FROM aeb
      WHERE aeb.event_start_date IS NOT NULL
      UNION ALL
      SELECT CAST(wmb.recorded_timestamp AS timestamp) AS data_updated_timestamp
      FROM wmb
      WHERE wmb.recorded_timestamp IS NOT NULL
    )
    SELECT
      MAX(data_updated_timestamp) AS data_updated_timestamp
    FROM unioned
    """
)

data_process_monitor_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/data_process_monitor_silver.csv"
)

data_process_monitor_silver_df.createOrReplaceTempView("data_process_monitor_silver")

# ============================================================
# TABLE: silver.report_latency_silver
# (UDT columns only: data_updated_timestamp)
# ============================================================
report_latency_silver_df = spark.sql(
    """
    SELECT
      CAST(dpms.data_updated_timestamp AS timestamp) AS data_updated_timestamp
    FROM patient_dashboard_metrics_silver pdms
    LEFT JOIN data_process_monitor_silver dpms
      ON 1 = 1
    WHERE dpms.data_updated_timestamp IS NOT NULL
    """
)

report_latency_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/report_latency_silver.csv"
)

report_latency_silver_df.createOrReplaceTempView("report_latency_silver")

# ============================================================
# TABLE: silver.patient_analytics_silver
# (No UDT columns provided for this target -> write empty select with no extra columns is invalid.
#  As per strict rule "Do not add new columns not mentioned in given columns", we will output patient_id only
#  from available UDT columns via join grain.
# ============================================================
patient_analytics_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        pis.patient_id AS patient_id,
        ROW_NUMBER() OVER (PARTITION BY pis.patient_id ORDER BY pis.patient_id) AS rn
      FROM patient_identity_silver pis
      LEFT JOIN patient_clinical_events_silver pces
        ON pis.patient_id = pces.patient_id
      WHERE pis.patient_id IS NOT NULL AND TRIM(pis.patient_id) <> ''
    )
    SELECT
      patient_id
    FROM base
    WHERE rn = 1
    """
)

patient_analytics_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_analytics_silver.csv"
)

job.commit()
