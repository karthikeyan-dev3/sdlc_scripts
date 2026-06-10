import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
patient_enrollment_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_silver.{FILE_FORMAT}/")
)

clinical_visits_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visits_silver.{FILE_FORMAT}/")
)

laboratory_tests_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/laboratory_tests_silver.{FILE_FORMAT}/")
)

drug_administration_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_silver.{FILE_FORMAT}/")
)

adverse_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)

wearable_device_data_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_device_data_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
patient_enrollment_silver_df.createOrReplaceTempView("patient_enrollment_silver")  # pes
clinical_visits_silver_df.createOrReplaceTempView("clinical_visits_silver")  # cvs
laboratory_tests_silver_df.createOrReplaceTempView("laboratory_tests_silver")  # lts
drug_administration_silver_df.createOrReplaceTempView("drug_administration_silver")  # das
adverse_events_silver_df.createOrReplaceTempView("adverse_events_silver")  # aes
wearable_device_data_silver_df.createOrReplaceTempView("wearable_device_data_silver")  # wds

# ----------------------------
# gold.gold_patient_enrollment (gpe) from patient_enrollment_silver (pes)
# ----------------------------
gold_patient_enrollment_df = spark.sql(
    """
    SELECT
        pes.patient_id AS patient_id,
        pes.enrollment_date AS enrollment_date,
        pes.study_id AS study_id,
        pes.demographics AS demographics,
        pes.standardized_patient_identifier AS standardized_patient_identifier
    FROM patient_enrollment_silver pes
    """
)

(
    gold_patient_enrollment_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_enrollment.csv")
)

# ----------------------------
# gold.gold_clinical_visits (gcv) from clinical_visits_silver (cvs)
# ----------------------------
gold_clinical_visits_df = spark.sql(
    """
    SELECT
        cvs.patient_id AS patient_id,
        cvs.visit_date AS visit_date,
        cvs.visit_type AS visit_type,
        cvs.standardized_patient_identifier AS standardized_patient_identifier
    FROM clinical_visits_silver cvs
    """
)

(
    gold_clinical_visits_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_visits.csv")
)

# ----------------------------
# gold.gold_laboratory_tests (glt) from laboratory_tests_silver (lts)
# ----------------------------
gold_laboratory_tests_df = spark.sql(
    """
    SELECT
        lts.patient_id AS patient_id,
        lts.test_date AS test_date,
        lts.test_type AS test_type,
        lts.test_result AS test_result,
        lts.standardized_patient_identifier AS standardized_patient_identifier
    FROM laboratory_tests_silver lts
    """
)

(
    gold_laboratory_tests_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_laboratory_tests.csv")
)

# ----------------------------
# gold.gold_drug_administration (gda) from drug_administration_silver (das)
# ----------------------------
gold_drug_administration_df = spark.sql(
    """
    SELECT
        das.patient_id AS patient_id,
        das.administration_date AS administration_date,
        das.drug_name AS drug_name,
        das.dosage AS dosage,
        das.standardized_patient_identifier AS standardized_patient_identifier
    FROM drug_administration_silver das
    """
)

(
    gold_drug_administration_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_administration.csv")
)

# ----------------------------
# gold.gold_adverse_events (gae) from adverse_events_silver (aes)
# ----------------------------
gold_adverse_events_df = spark.sql(
    """
    SELECT
        aes.patient_id AS patient_id,
        aes.event_date AS event_date,
        aes.event_type AS event_type,
        aes.severity AS severity,
        aes.standardized_patient_identifier AS standardized_patient_identifier
    FROM adverse_events_silver aes
    """
)

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# ----------------------------
# gold.gold_wearable_device_data (gwd) from wearable_device_data_silver (wds)
# ----------------------------
gold_wearable_device_data_df = spark.sql(
    """
    SELECT
        wds.patient_id AS patient_id,
        wds.data_timestamp AS data_timestamp,
        wds.device_type AS device_type,
        wds.heart_rate AS heart_rate,
        wds.activity_level AS activity_level,
        wds.standardized_patient_identifier AS standardized_patient_identifier
    FROM wearable_device_data_silver wds
    """
)

(
    gold_wearable_device_data_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_device_data.csv")
)
