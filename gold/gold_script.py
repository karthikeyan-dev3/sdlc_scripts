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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# -------------------------------------------------------------------
spe_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_enrollment.{FILE_FORMAT}/")
)
spe_df.createOrReplaceTempView("silver_patient_enrollment")

scv_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_clinical_visits.{FILE_FORMAT}/")
)
scv_df.createOrReplaceTempView("silver_clinical_visits")

slr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_lab_results.{FILE_FORMAT}/")
)
slr_df.createOrReplaceTempView("silver_lab_results")

sda_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_drug_administration.{FILE_FORMAT}/")
)
sda_df.createOrReplaceTempView("silver_drug_administration")

sae_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_adverse_events.{FILE_FORMAT}/")
)
sae_df.createOrReplaceTempView("silver_adverse_events")

swd_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_wearable_data.{FILE_FORMAT}/")
)
swd_df.createOrReplaceTempView("silver_wearable_data")

# -------------------------------------------------------------------
# Target: gold_patient_enrollment
# -------------------------------------------------------------------
gold_patient_enrollment_df = spark.sql(
    """
    SELECT
        CAST(spe.patient_id AS STRING) AS patient_id,
        CAST(spe.enrollment_date AS DATE) AS enrollment_date,
        CAST(spe.clinical_trial_id AS STRING) AS clinical_trial_id
    FROM silver_patient_enrollment spe
    """
)

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# -------------------------------------------------------------------
# Target: gold_clinical_visits
# -------------------------------------------------------------------
gold_clinical_visits_df = spark.sql(
    """
    SELECT
        CAST(scv.patient_id AS STRING) AS patient_id,
        CAST(scv.visit_date AS DATE) AS visit_date,
        CAST(scv.visit_type AS STRING) AS visit_type,
        CAST(scv.clinical_trial_id AS STRING) AS clinical_trial_id,
        CAST(scv.doctor_id AS STRING) AS doctor_id
    FROM silver_clinical_visits scv
    """
)

(
    gold_clinical_visits_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_visits.csv")
)

# -------------------------------------------------------------------
# Target: gold_lab_results
# -------------------------------------------------------------------
gold_lab_results_df = spark.sql(
    """
    SELECT
        CAST(slr.patient_id AS STRING) AS patient_id,
        CAST(slr.test_date AS DATE) AS test_date,
        CAST(slr.test_type AS STRING) AS test_type,
        CAST(slr.result_value AS DECIMAL(18,6)) AS result_value,
        CAST(slr.result_unit AS STRING) AS result_unit
    FROM silver_lab_results slr
    """
)

(
    gold_lab_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_results.csv")
)

# -------------------------------------------------------------------
# Target: gold_drug_administration
# -------------------------------------------------------------------
gold_drug_administration_df = spark.sql(
    """
    SELECT
        CAST(sda.patient_id AS STRING) AS patient_id,
        CAST(sda.administration_date AS DATE) AS administration_date,
        CAST(sda.drug_name AS STRING) AS drug_name,
        CAST(sda.dosage AS STRING) AS dosage,
        CAST(sda.route_of_administration AS STRING) AS route_of_administration
    FROM silver_drug_administration sda
    """
)

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# -------------------------------------------------------------------
# Target: gold_adverse_events
# -------------------------------------------------------------------
gold_adverse_events_df = spark.sql(
    """
    SELECT
        CAST(sae.patient_id AS STRING) AS patient_id,
        CAST(sae.event_date AS DATE) AS event_date,
        CAST(sae.event_description AS STRING) AS event_description,
        CAST(sae.severity AS STRING) AS severity,
        CAST(sae.clinical_trial_id AS STRING) AS clinical_trial_id
    FROM silver_adverse_events sae
    """
)

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# -------------------------------------------------------------------
# Target: gold_wearable_data
# -------------------------------------------------------------------
gold_wearable_data_df = spark.sql(
    """
    SELECT
        CAST(swd.patient_id AS STRING) AS patient_id,
        CAST(swd.data_timestamp AS TIMESTAMP) AS data_timestamp,
        CAST(swd.heart_rate AS INT) AS heart_rate,
        CAST(swd.steps_count AS INT) AS steps_count,
        CAST(swd.calorie_burn AS DECIMAL(18,6)) AS calorie_burn
    FROM silver_wearable_data swd
    """
)

(
    gold_wearable_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_data.csv")
)

# -------------------------------------------------------------------
# Target: gold_patient_summary
# -------------------------------------------------------------------
gold_patient_summary_df = spark.sql(
    """
    SELECT
        CAST(spe.patient_id AS STRING) AS patient_id,
        CAST(COUNT(DISTINCT spe.clinical_trial_id) AS INT) AS aggregate_clinical_trials,
        CAST(
            COUNT(
                DISTINCT CONCAT(
                    scv.patient_id, '|',
                    scv.clinical_trial_id, '|',
                    scv.visit_date, '|',
                    scv.visit_type, '|',
                    scv.doctor_id
                )
            ) AS INT
        ) AS total_visits,
        CAST(MAX(slr.test_date) AS DATE) AS last_lab_result_date,
        CAST(
            COUNT(
                DISTINCT CONCAT(
                    sae.patient_id, '|',
                    sae.clinical_trial_id, '|',
                    sae.event_date, '|',
                    sae.event_description
                )
            ) AS INT
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
    gold_patient_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_summary.csv")
)

job.commit()
