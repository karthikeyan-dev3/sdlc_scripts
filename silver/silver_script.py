import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables (Bronze)
# ------------------------------------------------------------------------------

patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)
patient_bronze_df.createOrReplaceTempView("patient_bronze")

patient_encounter_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_encounter_bronze.{FILE_FORMAT}/")
)
patient_encounter_bronze_df.createOrReplaceTempView("patient_encounter_bronze")

lab_result_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_bronze.{FILE_FORMAT}/")
)
lab_result_bronze_df.createOrReplaceTempView("lab_result_bronze")

drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
drug_administration_bronze_df.createOrReplaceTempView("drug_administration_bronze")

adverse_event_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_bronze.{FILE_FORMAT}/")
)
adverse_event_bronze_df.createOrReplaceTempView("adverse_event_bronze")

wearable_observation_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_observation_bronze.{FILE_FORMAT}/")
)
wearable_observation_bronze_df.createOrReplaceTempView("wearable_observation_bronze")

# ------------------------------------------------------------------------------
# patient_silver
# ------------------------------------------------------------------------------

patient_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(pb.transaction_id AS STRING) AS source_patient_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(pb.transaction_id AS STRING)
      ORDER BY CAST(pb.transaction_id AS STRING)
    ) AS rn
  FROM patient_bronze pb
)
SELECT
  source_patient_id
FROM ranked
WHERE rn = 1
""")

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

patient_silver_df.createOrReplaceTempView("patient_silver")

# ------------------------------------------------------------------------------
# patient_encounter_silver
# ------------------------------------------------------------------------------

patient_encounter_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(peb.transaction_id AS STRING) AS source_encounter_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(peb.transaction_id AS STRING)
      ORDER BY CAST(peb.transaction_id AS STRING)
    ) AS rn
  FROM patient_encounter_bronze peb
)
SELECT
  source_encounter_id
FROM ranked
WHERE rn = 1
""")

(
    patient_encounter_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_encounter_silver.csv")
)

patient_encounter_silver_df.createOrReplaceTempView("patient_encounter_silver")

# ------------------------------------------------------------------------------
# lab_result_silver
# ------------------------------------------------------------------------------

lab_result_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(lrb.inventory_event_id AS STRING) AS source_lab_result_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(lrb.inventory_event_id AS STRING)
      ORDER BY CAST(lrb.inventory_event_id AS STRING)
    ) AS rn
  FROM lab_result_bronze lrb
)
SELECT
  source_lab_result_id
FROM ranked
WHERE rn = 1
""")

(
    lab_result_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_result_silver.csv")
)

lab_result_silver_df.createOrReplaceTempView("lab_result_silver")

# ------------------------------------------------------------------------------
# drug_administration_silver
# ------------------------------------------------------------------------------

drug_administration_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(dab.payment_id AS STRING) AS source_drug_admin_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(dab.payment_id AS STRING)
      ORDER BY CAST(dab.payment_id AS STRING)
    ) AS rn
  FROM drug_administration_bronze dab
)
SELECT
  source_drug_admin_id
FROM ranked
WHERE rn = 1
""")

(
    drug_administration_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_administration_silver.csv")
)

drug_administration_silver_df.createOrReplaceTempView("drug_administration_silver")

# ------------------------------------------------------------------------------
# adverse_event_silver
# ------------------------------------------------------------------------------

adverse_event_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(aeb.transaction_id AS STRING) AS source_adverse_event_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(aeb.transaction_id AS STRING)
      ORDER BY CAST(aeb.transaction_id AS STRING)
    ) AS rn
  FROM adverse_event_bronze aeb
)
SELECT
  source_adverse_event_id
FROM ranked
WHERE rn = 1
""")

(
    adverse_event_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_event_silver.csv")
)

adverse_event_silver_df.createOrReplaceTempView("adverse_event_silver")

# ------------------------------------------------------------------------------
# wearable_observation_silver
# ------------------------------------------------------------------------------

wearable_observation_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(wob.footfall_event_id AS STRING) AS source_device_id,
    CAST(wob.entry_count AS INT) AS metric_value,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(wob.footfall_event_id AS STRING)
      ORDER BY CAST(wob.footfall_event_id AS STRING)
    ) AS rn
  FROM wearable_observation_bronze wob
)
SELECT
  source_device_id,
  metric_value
FROM ranked
WHERE rn = 1
""")

(
    wearable_observation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/wearable_observation_silver.csv")
)

wearable_observation_silver_df.createOrReplaceTempView("wearable_observation_silver")

# ------------------------------------------------------------------------------
# patient_360_timeline_silver (per provided mapping_details joins)
# ------------------------------------------------------------------------------

patient_360_timeline_silver_df = spark.sql("""
SELECT
  CAST(ps.source_patient_id AS STRING) AS source_patient_id,
  CAST(pes.source_encounter_id AS STRING) AS source_encounter_id,
  CAST(lrs.source_lab_result_id AS STRING) AS source_lab_result_id,
  CAST(das.source_drug_admin_id AS STRING) AS source_drug_admin_id,
  CAST(aes.source_adverse_event_id AS STRING) AS source_adverse_event_id,
  CAST(wos.source_device_id AS STRING) AS source_device_id,
  CAST(wos.metric_value AS INT) AS metric_value
FROM patient_silver ps
LEFT JOIN patient_encounter_silver pes
  ON ps.source_patient_id = pes.patient_id
LEFT JOIN lab_result_silver lrs
  ON ps.source_patient_id = lrs.patient_id
LEFT JOIN drug_administration_silver das
  ON ps.source_patient_id = das.patient_id
LEFT JOIN adverse_event_silver aes
  ON ps.source_patient_id = aes.patient_id
LEFT JOIN wearable_observation_silver wos
  ON ps.source_patient_id = wos.patient_id
""")

(
    patient_360_timeline_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_360_timeline_silver.csv")
)