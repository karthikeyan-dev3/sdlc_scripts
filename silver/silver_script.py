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

# -----------------------------
# Read Source Tables (S3 -> DF)
# -----------------------------
sequencing_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_runs_bronze.{FILE_FORMAT}/")
)

lab_work_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_work_items_bronze.{FILE_FORMAT}/")
)

pending_approvals_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/pending_approvals_bronze.{FILE_FORMAT}/")
)

patient_diagnostic_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_diagnostic_results_bronze.{FILE_FORMAT}/")
)

pathogenic_variant_alerts_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/pathogenic_variant_alerts_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
sequencing_runs_bronze_df.createOrReplaceTempView("sequencing_runs_bronze")
lab_work_items_bronze_df.createOrReplaceTempView("lab_work_items_bronze")
pending_approvals_bronze_df.createOrReplaceTempView("pending_approvals_bronze")
patient_diagnostic_results_bronze_df.createOrReplaceTempView("patient_diagnostic_results_bronze")
pathogenic_variant_alerts_bronze_df.createOrReplaceTempView("pathogenic_variant_alerts_bronze")

# ----------------------------------------
# Transform + Write: sequencing_runs_silver
# ----------------------------------------
sequencing_runs_silver_df = spark.sql(
    """
    SELECT
        srb.run_id AS run_id,
        srb.patient_id AS patient_id,
        srb.sample_id AS sample_id,
        srb.sequencing_platform AS sequencing_platform,
        CAST(srb.run_date AS DATE) AS run_date,
        srb.processing_status AS processing_status,
        srb.upload_timestamp AS upload_timestamp
    FROM sequencing_runs_bronze srb
    """
)

(
    sequencing_runs_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_runs_silver.csv")
)

# ---------------------------------------
# Transform + Write: lab_work_items_silver
# ---------------------------------------
lab_work_items_silver_df = spark.sql(
    """
    SELECT
        lwib.result_id AS result_id,
        lwib.patient_id AS patient_id,
        lwib.sample_id AS sample_id,
        lwib.test_name AS test_name,
        lwib.biomarker AS biomarker,
        lwib.test_result AS test_result,
        lwib.unit AS unit,
        CAST(lwib.collection_date AS DATE) AS collection_date,
        CAST(lwib.result_date AS DATE) AS result_date
    FROM lab_work_items_bronze lwib
    """
)

(
    lab_work_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_work_items_silver.csv")
)

# ------------------------------------------
# Transform + Write: pending_approvals_silver
# ------------------------------------------
pending_approvals_silver_df = spark.sql(
    """
    SELECT
        pab.approval_status AS approval_status,
        pab.result_id AS result_id,
        pab.patient_id AS patient_id,
        CAST(pab.result_date AS DATE) AS result_date
    FROM pending_approvals_bronze pab
    """
)

(
    pending_approvals_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pending_approvals_silver.csv")
)

# ----------------------------------------------------
# Transform + Write: patient_diagnostic_results_silver
# ----------------------------------------------------
patient_diagnostic_results_silver_df = spark.sql(
    """
    SELECT
        pdrb.patient_id AS patient_id,
        pdrb.diagnosis AS diagnosis,
        CAST(pdrb.registration_date AS DATE) AS registration_date
    FROM patient_diagnostic_results_bronze pdrb
    """
)

(
    patient_diagnostic_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_diagnostic_results_silver.csv")
)

# -------------------------------------------------
# Transform + Write: pathogenic_variant_alerts_silver
# -------------------------------------------------
pathogenic_variant_alerts_silver_df = spark.sql(
    """
    SELECT
        pvab.variant_id AS variant_id,
        pvab.patient_id AS patient_id,
        pvab.run_id AS run_id,
        pvab.gene_name AS gene_name,
        pvab.variant_type AS variant_type,
        pvab.clinical_significance AS clinical_significance,
        CAST(pvab.pathogenicity_score AS FLOAT) AS pathogenicity_score,
        CAST(pvab.detected_date AS DATE) AS detected_date,
        pvab.validation_status AS validation_status,
        pvab.reporting_lab AS reporting_lab
    FROM pathogenic_variant_alerts_bronze pvab
    """
)

(
    pathogenic_variant_alerts_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pathogenic_variant_alerts_silver.csv")
)