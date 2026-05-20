import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables
# --------------------------------------------------------------------------------------
pes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_silver.{FILE_FORMAT}/")
)
cvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_silver.{FILE_FORMAT}/")
)
lrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_silver.{FILE_FORMAT}/")
)
das_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_silver.{FILE_FORMAT}/")
)
aes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)
wms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
pes_df.createOrReplaceTempView("pes")
cvs_df.createOrReplaceTempView("cvs")
lrs_df.createOrReplaceTempView("lrs")
das_df.createOrReplaceTempView("das")
aes_df.createOrReplaceTempView("aes")
wms_df.createOrReplaceTempView("wms")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save output (each target table separately)
# --------------------------------------------------------------------------------------

# gold_patient_enrollment
gold_patient_enrollment_df = spark.sql("""
SELECT
  CAST(pes.patient_id AS STRING) AS patient_id,
  CAST(pes.enrollment_date AS TIMESTAMP) AS enrollment_date,
  CAST(pes.trial_id AS STRING) AS study_id,
  CAST(pes.patient_name AS STRING) AS demographics,
  CAST(pes.patient_id AS STRING) AS standardized_patient_identifier
FROM pes
""")

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# gold_clinical_visits
gold_clinical_visits_df = spark.sql("""
SELECT
  CAST(cvs.patient_id AS STRING) AS patient_id,
  CAST(cvs.visit_date AS TIMESTAMP) AS visit_date,
  CAST(cvs.visit_type AS STRING) AS visit_type,
  CAST(cvs.patient_id AS STRING) AS standardized_patient_identifier
FROM cvs
LEFT JOIN pes
  ON cvs.patient_id = pes.patient_id
 AND cvs.trial_id = pes.trial_id
""")

(
    gold_clinical_visits_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_visits.csv")
)

# gold_laboratory_tests
gold_laboratory_tests_df = spark.sql("""
SELECT
  CAST(lrs.patient_id AS STRING) AS patient_id,
  CAST(lrs.test_date AS TIMESTAMP) AS test_date,
  CAST(lrs.test_name AS STRING) AS test_type,
  CAST(lrs.test_result AS DOUBLE) AS test_result,
  CAST(lrs.patient_id AS STRING) AS standardized_patient_identifier
FROM lrs
LEFT JOIN pes
  ON lrs.patient_id = pes.patient_id
""")

(
    gold_laboratory_tests_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_laboratory_tests.csv")
)

# gold_drug_administration
gold_drug_administration_df = spark.sql("""
SELECT
  CAST(das.patient_id AS STRING) AS patient_id,
  CAST(das.administration_date AS TIMESTAMP) AS administration_date,
  CAST(das.drug_code AS STRING) AS drug_name,
  CAST(das.dosage_mg AS DOUBLE) AS dosage,
  CAST(das.patient_id AS STRING) AS standardized_patient_identifier
FROM das
LEFT JOIN pes
  ON das.patient_id = pes.patient_id
""")

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# gold_adverse_events
gold_adverse_events_df = spark.sql("""
SELECT
  CAST(aes.patient_id AS STRING) AS patient_id,
  CAST(aes.event_start_date AS TIMESTAMP) AS event_date,
  CAST(aes.event_type AS STRING) AS event_type,
  CAST(aes.severity AS STRING) AS severity,
  CAST(aes.patient_id AS STRING) AS standardized_patient_identifier
FROM aes
LEFT JOIN pes
  ON aes.patient_id = pes.patient_id
""")

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# gold_wearable_device_data
gold_wearable_device_data_df = spark.sql("""
SELECT
  CAST(wms.patient_id AS STRING) AS patient_id,
  CAST(wms.recorded_timestamp AS TIMESTAMP) AS data_timestamp,
  CAST(wms.device_type AS STRING) AS device_type,
  CAST(wms.heart_rate AS DOUBLE) AS heart_rate,
  CAST(wms.patient_id AS STRING) AS standardized_patient_identifier
FROM wms
LEFT JOIN pes
  ON wms.patient_id = pes.patient_id
""")

(
    gold_wearable_device_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_device_data.csv")
)

job.commit()