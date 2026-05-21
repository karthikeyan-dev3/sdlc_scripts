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

# ------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)

patient_encounter_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_encounter_silver.{FILE_FORMAT}/")
)

lab_result_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_silver.{FILE_FORMAT}/")
)

drug_administration_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_silver.{FILE_FORMAT}/")
)

adverse_event_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_silver.{FILE_FORMAT}/")
)

wearable_observation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_observation_silver.{FILE_FORMAT}/")
)

patient_360_timeline_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_360_timeline_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------
patient_silver_df.createOrReplaceTempView("patient_silver")
patient_encounter_silver_df.createOrReplaceTempView("patient_encounter_silver")
lab_result_silver_df.createOrReplaceTempView("lab_result_silver")
drug_administration_silver_df.createOrReplaceTempView("drug_administration_silver")
adverse_event_silver_df.createOrReplaceTempView("adverse_event_silver")
wearable_observation_silver_df.createOrReplaceTempView("wearable_observation_silver")
patient_360_timeline_silver_df.createOrReplaceTempView("patient_360_timeline_silver")

# ------------------------------------------------------------------
# 3) Transform + 4) Save output (each target table separately)
# ------------------------------------------------------------------

# gold.gold_patient
gold_patient_df = spark.sql(
    """
    SELECT
        CAST(ps.source_patient_id AS STRING) AS source_patient_id,
        CAST(ps.record_effective_start_ts AS TIMESTAMP) AS record_effective_start_ts,
        CAST(ps.record_effective_end_ts AS TIMESTAMP) AS record_effective_end_ts,
        CAST(ps.is_current_record AS BOOLEAN) AS is_current_record
    FROM patient_silver ps
    """
)

(
    gold_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient.csv")
)

# gold.gold_patient_encounter
gold_patient_encounter_df = spark.sql(
    """
    SELECT
        CAST(pes.source_encounter_id AS STRING) AS source_encounter_id
    FROM patient_encounter_silver pes
    INNER JOIN patient_silver ps
        ON pes.patient_id = ps.source_patient_id
    """
)

(
    gold_patient_encounter_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_encounter.csv")
)

# gold.gold_lab_result
gold_lab_result_df = spark.sql(
    """
    SELECT
        CAST(lrs.source_lab_result_id AS STRING) AS source_lab_result_id
    FROM lab_result_silver lrs
    INNER JOIN patient_silver ps
        ON lrs.patient_id = ps.source_patient_id
    LEFT JOIN patient_encounter_silver pes
        ON lrs.encounter_id = pes.source_encounter_id
    """
)

(
    gold_lab_result_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_lab_result.csv")
)

# gold.gold_drug_administration
gold_drug_administration_df = spark.sql(
    """
    SELECT
        CAST(das.source_drug_admin_id AS STRING) AS source_drug_admin_id
    FROM drug_administration_silver das
    INNER JOIN patient_silver ps
        ON das.patient_id = ps.source_patient_id
    LEFT JOIN patient_encounter_silver pes
        ON das.encounter_id = pes.source_encounter_id
    """
)

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# gold.gold_adverse_event
gold_adverse_event_df = spark.sql(
    """
    SELECT
        CAST(aes.source_adverse_event_id AS STRING) AS source_adverse_event_id
    FROM adverse_event_silver aes
    INNER JOIN patient_silver ps
        ON aes.patient_id = ps.source_patient_id
    LEFT JOIN patient_encounter_silver pes
        ON aes.encounter_id = pes.source_encounter_id
    """
)

(
    gold_adverse_event_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_event.csv")
)

# gold.gold_wearable_observation
gold_wearable_observation_df = spark.sql(
    """
    SELECT
        CAST(wos.source_device_id AS STRING) AS source_device_id,
        CAST(wos.metric_value AS INT) AS metric_value
    FROM wearable_observation_silver wos
    INNER JOIN patient_silver ps
        ON wos.patient_id = ps.source_patient_id
    """
)

(
    gold_wearable_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_observation.csv")
)

# gold.gold_patient_360_timeline
gold_patient_360_timeline_df = spark.sql(
    """
    SELECT
        1 AS placeholder
    FROM patient_360_timeline_silver p360s
    INNER JOIN patient_silver ps
        ON p360s.patient_id = ps.source_patient_id
    LEFT JOIN patient_encounter_silver pes
        ON p360s.encounter_id = pes.source_encounter_id
    """
)

(
    gold_patient_360_timeline_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_360_timeline.csv")
)

job.commit()