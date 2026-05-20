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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read Source Tables (S3)
# ----------------------------
pes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_silver.{FILE_FORMAT}/")
)
pes_df.createOrReplaceTempView("patient_enrollment_silver")

lrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_silver.{FILE_FORMAT}/")
)
lrs_df.createOrReplaceTempView("lab_results_silver")

aes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)
aes_df.createOrReplaceTempView("adverse_events_silver")

wds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_device_data_silver.{FILE_FORMAT}/")
)
wds_df.createOrReplaceTempView("wearable_device_data_silver")

mls_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/metadata_lineage_silver.{FILE_FORMAT}/")
)
mls_df.createOrReplaceTempView("metadata_lineage_silver")

acs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/access_control_silver.{FILE_FORMAT}/")
)
acs_df.createOrReplaceTempView("access_control_silver")

# ----------------------------
# Target: gold_patient_enrollment
# ----------------------------
gold_patient_enrollment_df = spark.sql(
    """
    SELECT
        CAST(pes.enrollment_id AS STRING)   AS enrollment_id,
        CAST(pes.patient_id AS STRING)      AS patient_id,
        CAST(pes.trial_id AS STRING)        AS trial_id,
        CAST(pes.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(pes.status AS STRING)          AS status
    FROM patient_enrollment_silver pes
    """
)

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# ----------------------------
# Target: gold_lab_results
# ----------------------------
gold_lab_results_df = spark.sql(
    """
    SELECT
        CAST(lrs.result_id AS STRING)          AS result_id,
        CAST(lrs.patient_id AS STRING)         AS patient_id,
        CAST(lrs.trial_id AS STRING)           AS trial_id,
        CAST(lrs.result_date AS TIMESTAMP)     AS result_date,
        CAST(lrs.test_type AS STRING)          AS test_type,
        CAST(lrs.result_value AS DOUBLE)       AS result_value,
        CAST(lrs.unit AS STRING)               AS unit,
        CAST(lrs.status AS STRING)             AS status
    FROM lab_results_silver lrs
    LEFT JOIN patient_enrollment_silver pes
        ON lrs.patient_id = pes.patient_id
       AND lrs.trial_id = pes.trial_id
    """
)

(
    gold_lab_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_results.csv")
)

# ----------------------------
# Target: gold_adverse_events
# ----------------------------
gold_adverse_events_df = spark.sql(
    """
    SELECT
        CAST(aes.event_id AS STRING)              AS event_id,
        CAST(aes.patient_id AS STRING)            AS patient_id,
        CAST(aes.trial_id AS STRING)              AS trial_id,
        CAST(aes.event_date AS TIMESTAMP)         AS event_date,
        CAST(aes.event_type AS STRING)            AS event_type,
        CAST(aes.severity AS STRING)              AS severity,
        CAST(aes.related_to_study_drug AS BOOLEAN) AS related_to_study_drug
    FROM adverse_events_silver aes
    LEFT JOIN patient_enrollment_silver pes
        ON aes.patient_id = pes.patient_id
       AND aes.trial_id = pes.trial_id
    """
)

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# ----------------------------
# Target: gold_wearable_device_data
# ----------------------------
gold_wearable_device_data_df = spark.sql(
    """
    SELECT
        CAST(wds.device_id AS STRING)            AS device_id,
        CAST(wds.patient_id AS STRING)           AS patient_id,
        CAST(wds.trial_id AS STRING)             AS trial_id,
        CAST(wds.measurement_date AS TIMESTAMP)  AS measurement_date,
        CAST(wds.metric_type AS STRING)          AS metric_type,
        CAST(wds.metric_value AS DOUBLE)         AS metric_value,
        CAST(wds.unit AS STRING)                 AS unit
    FROM wearable_device_data_silver wds
    LEFT JOIN patient_enrollment_silver pes
        ON wds.patient_id = pes.patient_id
       AND wds.trial_id = pes.trial_id
    """
)

(
    gold_wearable_device_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_device_data.csv")
)

# ----------------------------
# Target: gold_metadata_lineage
# ----------------------------
gold_metadata_lineage_df = spark.sql(
    """
    SELECT
        *
    FROM metadata_lineage_silver mls
    """
)

(
    gold_metadata_lineage_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_metadata_lineage.csv")
)

# ----------------------------
# Target: gold_access_control
# ----------------------------
gold_access_control_df = spark.sql(
    """
    SELECT
        *
    FROM access_control_silver acs
    """
)

(
    gold_access_control_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_access_control.csv")
)

job.commit()
