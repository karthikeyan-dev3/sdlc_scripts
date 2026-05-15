import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Read Source Tables (Bronze) and Create Temp Views
# -------------------------------------------------------------------
bronze_patient_enrollment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_patient_enrollment_bronze.{FILE_FORMAT}/")
)
bronze_patient_enrollment_bronze_df.createOrReplaceTempView("bronze_patient_enrollment_bronze")

bronze_clinical_visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_clinical_visits_bronze.{FILE_FORMAT}/")
)
bronze_clinical_visits_bronze_df.createOrReplaceTempView("bronze_clinical_visits_bronze")

bronze_lab_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_lab_results_bronze.{FILE_FORMAT}/")
)
bronze_lab_results_bronze_df.createOrReplaceTempView("bronze_lab_results_bronze")

bronze_drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_drug_administration_bronze.{FILE_FORMAT}/")
)
bronze_drug_administration_bronze_df.createOrReplaceTempView("bronze_drug_administration_bronze")

bronze_adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_adverse_events_bronze.{FILE_FORMAT}/")
)
bronze_adverse_events_bronze_df.createOrReplaceTempView("bronze_adverse_events_bronze")

bronze_wearable_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_wearable_data_bronze.{FILE_FORMAT}/")
)
bronze_wearable_data_bronze_df.createOrReplaceTempView("bronze_wearable_data_bronze")

# -------------------------------------------------------------------
# Target: silver_patient_enrollment
# -------------------------------------------------------------------
silver_patient_enrollment_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(peb.patient_id)) AS patient_id,
            TRIM(UPPER(peb.clinical_trial_id)) AS clinical_trial_id,
            CAST(peb.enrollment_date AS DATE) AS enrollment_date,
            ROW_NUMBER() OVER (
                PARTITION BY peb.patient_id, peb.clinical_trial_id, peb.enrollment_date
                ORDER BY peb.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_patient_enrollment_bronze peb
        WHERE peb.is_deleted = false
    )
    SELECT
        patient_id,
        clinical_trial_id,
        enrollment_date
    FROM ranked
    WHERE rn = 1
    """
)
silver_patient_enrollment_df.createOrReplaceTempView("silver_patient_enrollment")

(
    silver_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_patient_enrollment.csv")
)

# -------------------------------------------------------------------
# Target: silver_clinical_visits
# -------------------------------------------------------------------
silver_clinical_visits_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(cvb.patient_id)) AS patient_id,
            TRIM(UPPER(cvb.clinical_trial_id)) AS clinical_trial_id,
            CAST(cvb.visit_date AS DATE) AS visit_date,
            TRIM(UPPER(cvb.visit_type)) AS visit_type,
            TRIM(UPPER(cvb.doctor_id)) AS doctor_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cvb.patient_id,
                    cvb.clinical_trial_id,
                    cvb.visit_date,
                    cvb.visit_type,
                    cvb.doctor_id
                ORDER BY cvb.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_clinical_visits_bronze cvb
        WHERE cvb.is_deleted = false
    )
    SELECT
        patient_id,
        clinical_trial_id,
        visit_date,
        visit_type,
        doctor_id
    FROM ranked
    WHERE rn = 1
    """
)
silver_clinical_visits_df.createOrReplaceTempView("silver_clinical_visits")

(
    silver_clinical_visits_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_clinical_visits.csv")
)

# -------------------------------------------------------------------
# Target: silver_lab_results
# -------------------------------------------------------------------
silver_lab_results_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(lrb.patient_id)) AS patient_id,
            CAST(lrb.test_date AS DATE) AS test_date,
            TRIM(UPPER(lrb.test_type)) AS test_type,
            CAST(lrb.result_value AS DECIMAL(18,6)) AS result_value,
            TRIM(UPPER(lrb.result_unit)) AS result_unit,
            ROW_NUMBER() OVER (
                PARTITION BY lrb.patient_id, lrb.test_date, lrb.test_type
                ORDER BY lrb.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_lab_results_bronze lrb
        WHERE lrb.is_deleted = false
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
silver_lab_results_df.createOrReplaceTempView("silver_lab_results")

(
    silver_lab_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_lab_results.csv")
)

# -------------------------------------------------------------------
# Target: silver_drug_administration
# -------------------------------------------------------------------
silver_drug_administration_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(dab.patient_id)) AS patient_id,
            CAST(dab.administration_date AS DATE) AS administration_date,
            TRIM(UPPER(dab.drug_name)) AS drug_name,
            TRIM(UPPER(dab.dosage)) AS dosage,
            TRIM(UPPER(dab.route_of_administration)) AS route_of_administration,
            ROW_NUMBER() OVER (
                PARTITION BY
                    dab.patient_id,
                    dab.administration_date,
                    dab.drug_name,
                    dab.dosage,
                    dab.route_of_administration
                ORDER BY dab.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_drug_administration_bronze dab
        WHERE dab.is_deleted = false
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
silver_drug_administration_df.createOrReplaceTempView("silver_drug_administration")

(
    silver_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_drug_administration.csv")
)

# -------------------------------------------------------------------
# Target: silver_adverse_events
# -------------------------------------------------------------------
silver_adverse_events_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(aeb.patient_id)) AS patient_id,
            TRIM(UPPER(aeb.clinical_trial_id)) AS clinical_trial_id,
            CAST(aeb.event_date AS DATE) AS event_date,
            TRIM(aeb.event_description) AS event_description,
            TRIM(UPPER(aeb.severity)) AS severity,
            ROW_NUMBER() OVER (
                PARTITION BY
                    aeb.patient_id,
                    aeb.clinical_trial_id,
                    aeb.event_date,
                    aeb.event_description
                ORDER BY aeb.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_adverse_events_bronze aeb
        WHERE aeb.is_deleted = false
    )
    SELECT
        patient_id,
        clinical_trial_id,
        event_date,
        event_description,
        severity
    FROM ranked
    WHERE rn = 1
    """
)
silver_adverse_events_df.createOrReplaceTempView("silver_adverse_events")

(
    silver_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_adverse_events.csv")
)

# -------------------------------------------------------------------
# Target: silver_wearable_data
# -------------------------------------------------------------------
silver_wearable_data_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(UPPER(wdb.patient_id)) AS patient_id,
            CAST(wdb.data_timestamp AS TIMESTAMP) AS data_timestamp,
            CAST(wdb.heart_rate AS INT) AS heart_rate,
            CAST(wdb.steps_count AS INT) AS steps_count,
            CAST(wdb.calorie_burn AS DECIMAL(18,6)) AS calorie_burn,
            ROW_NUMBER() OVER (
                PARTITION BY wdb.patient_id, wdb.data_timestamp
                ORDER BY wdb.ingestion_timestamp DESC
            ) AS rn
        FROM bronze_wearable_data_bronze wdb
        WHERE wdb.is_deleted = false
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
silver_wearable_data_df.createOrReplaceTempView("silver_wearable_data")

(
    silver_wearable_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_wearable_data.csv")
)

# -------------------------------------------------------------------
# Target: silver_patient_summary
# -------------------------------------------------------------------
silver_patient_summary_df = spark.sql(
    """
    SELECT
        spe.patient_id AS patient_id,
        COUNT(DISTINCT spe.clinical_trial_id) AS aggregate_clinical_trials,
        COUNT(
            DISTINCT CONCAT(
                scv.patient_id, '|',
                scv.clinical_trial_id, '|',
                scv.visit_date, '|',
                scv.visit_type, '|',
                scv.doctor_id
            )
        ) AS total_visits,
        MAX(slr.test_date) AS last_lab_result_date,
        COUNT(
            DISTINCT CONCAT(
                sae.patient_id, '|',
                sae.clinical_trial_id, '|',
                sae.event_date, '|',
                sae.event_description
            )
        ) AS adverse_event_count
    FROM silver_patient_enrollment spe
    LEFT JOIN silver_clinical_visits scv
        ON spe.patient_id = scv.patient_id
    LEFT JOIN silver_lab_results slr
        ON spe.patient_id = slr.patient_id
    LEFT JOIN silver_adverse_events sae
        ON spe.patient_id = sae.patient_id
    GROUP BY spe.patient_id
    """
)

(
    silver_patient_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_patient_summary.csv")
)

job.commit()