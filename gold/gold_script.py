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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# Source Reads + Temp Views
# --------------------------------------------------------------------

pdus_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_unified_silver.{FILE_FORMAT}/")
)
pdus_df.createOrReplaceTempView("patient_data_unified_silver")

cds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_dashboard_silver.{FILE_FORMAT}/")
)
cds_df.createOrReplaceTempView("clinical_dashboard_silver")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/safety_monitoring_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("safety_monitoring_silver")

dqs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)
dqs_df.createOrReplaceTempView("data_quality_silver")

wds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_data_silver.{FILE_FORMAT}/")
)
wds_df.createOrReplaceTempView("wearable_data_silver")

# --------------------------------------------------------------------
# Target: gold_patient_data
# --------------------------------------------------------------------

gold_patient_data_df = spark.sql(
    """
    SELECT
        CAST(pdus.patient_id AS STRING) AS patient_id,
        CAST(pdus.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(pdus.clinical_visit_date AS TIMESTAMP) AS clinical_visit_date,
        CAST(pdus.laboratory_test_id AS STRING) AS laboratory_test_id,
        CAST(pdus.drug_administration_id AS STRING) AS drug_administration_id,
        CAST(pdus.adverse_event_id AS STRING) AS adverse_event_id,
        CAST(pdus.wearable_device_data AS STRING) AS wearable_device_data
    FROM patient_data_unified_silver pdus
    """
)

(
    gold_patient_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_data.csv")
)

# --------------------------------------------------------------------
# Target: gold_clinical_dashboard
# --------------------------------------------------------------------

gold_clinical_dashboard_df = spark.sql(
    """
    SELECT
        CAST(cds.patient_id AS STRING) AS patient_id,
        CAST(cds.analysis_date AS TIMESTAMP) AS analysis_date
    FROM clinical_dashboard_silver cds
    """
)

(
    gold_clinical_dashboard_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_dashboard.csv")
)

# --------------------------------------------------------------------
# Target: gold_safety_monitoring
# --------------------------------------------------------------------

gold_safety_monitoring_df = spark.sql(
    """
    SELECT
        CAST(sms.patient_id AS STRING) AS patient_id
    FROM safety_monitoring_silver sms
    """
)

(
    gold_safety_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_safety_monitoring.csv")
)

# --------------------------------------------------------------------
# Target: gold_data_quality
# --------------------------------------------------------------------

gold_data_quality_df = spark.sql(
    """
    SELECT
        *
    FROM data_quality_silver dqs
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

# --------------------------------------------------------------------
# Target: gold_wearable_data
# --------------------------------------------------------------------

gold_wearable_data_df = spark.sql(
    """
    SELECT
        CAST(wds.patient_id AS STRING) AS patient_id,
        CAST(wds.wearable_device_id AS STRING) AS wearable_device_id,
        CAST(wds.data_timestamp AS TIMESTAMP) AS data_timestamp,
        CAST(wds.heart_rate AS DOUBLE) AS heart_rate,
        CAST(wds.activity_level AS INT) AS activity_level
    FROM wearable_data_silver wds
    """
)

(
    gold_wearable_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_data.csv")
)

job.commit()
