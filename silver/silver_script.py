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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) READ SOURCE TABLES (BRONZE) + CREATE TEMP VIEWS
# ------------------------------------------------------------------------------

adverse_event_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_bronze.{FILE_FORMAT}/")
)
adverse_event_bronze_df.createOrReplaceTempView("adverse_event_bronze")

patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)
patient_bronze_df.createOrReplaceTempView("patient_bronze")

drug_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_bronze.{FILE_FORMAT}/")
)
drug_bronze_df.createOrReplaceTempView("drug_bronze")

site_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_bronze.{FILE_FORMAT}/")
)
site_bronze_df.createOrReplaceTempView("site_bronze")

# ------------------------------------------------------------------------------
# 2) adverse_event_silver (DEDUP by ae_id using ROW_NUMBER)
# ------------------------------------------------------------------------------

adverse_event_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(aeb.ae_id) AS STRING) AS ae_id,
            CAST(TRIM(aeb.patient_id) AS STRING) AS patient_id,
            CAST(TRIM(aeb.drug_id) AS STRING) AS drug_id,
            CAST(TRIM(aeb.site_id) AS STRING) AS site_id,
            CAST(TRIM(aeb.symptom) AS STRING) AS event_type,
            DATE(aeb.report_date) AS event_date,
            CAST(TRIM(aeb.severity) AS STRING) AS severity,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(aeb.ae_id)
                ORDER BY TRIM(aeb.ae_id)
            ) AS rn
        FROM adverse_event_bronze aeb
    )
    SELECT
        ae_id,
        patient_id,
        drug_id,
        site_id,
        event_type,
        event_date,
        severity
    FROM ranked
    WHERE rn = 1
    """
)
adverse_event_silver_df.createOrReplaceTempView("adverse_event_silver")

(
    adverse_event_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/adverse_event_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) patient_silver (DEDUP by patient_id using ROW_NUMBER)
# ------------------------------------------------------------------------------

patient_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(pb.patient_id) AS STRING) AS patient_id,
            CAST(pb.age AS INT) AS age,
            CAST(TRIM(pb.gender) AS STRING) AS gender,
            DATE(pb.enrollment_date) AS enrollment_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.patient_id)
                ORDER BY TRIM(pb.patient_id)
            ) AS rn
        FROM patient_bronze pb
    )
    SELECT
        patient_id,
        age,
        gender,
        enrollment_date
    FROM ranked
    WHERE rn = 1
    """
)
patient_silver_df.createOrReplaceTempView("patient_silver")

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

# ------------------------------------------------------------------------------
# 4) drug_silver (DEDUP by drug_id using ROW_NUMBER)
# ------------------------------------------------------------------------------

drug_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(db.drug_id) AS STRING) AS drug_id,
            CAST(TRIM(db.drug_name) AS STRING) AS drug_name,
            CAST(TRIM(db.drug_category) AS STRING) AS drug_category,
            CAST(TRIM(db.manufacturer) AS STRING) AS manufacturer,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(db.drug_id)
                ORDER BY TRIM(db.drug_id)
            ) AS rn
        FROM drug_bronze db
    )
    SELECT
        drug_id,
        drug_name,
        drug_category,
        manufacturer
    FROM ranked
    WHERE rn = 1
    """
)
drug_silver_df.createOrReplaceTempView("drug_silver")

(
    drug_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/drug_silver.csv")
)

# ------------------------------------------------------------------------------
# 5) site_silver (DEDUP by site_id using ROW_NUMBER)
# ------------------------------------------------------------------------------

site_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(sb.site_id) AS STRING) AS site_id,
            CAST(TRIM(sb.city) AS STRING) AS site_location,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.site_id)
                ORDER BY TRIM(sb.site_id)
            ) AS rn
        FROM site_bronze sb
    )
    SELECT
        site_id,
        site_location
    FROM ranked
    WHERE rn = 1
    """
)
site_silver_df.createOrReplaceTempView("site_silver")

(
    site_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_silver.csv")
)

# ------------------------------------------------------------------------------
# 6) aggregated_dataset_silver (report_date = aes.event_date)
# ------------------------------------------------------------------------------

aggregated_dataset_silver_df = spark.sql(
    """
    SELECT
        DATE(aes.event_date) AS report_date
    FROM adverse_event_silver aes
    LEFT JOIN patient_silver ps
        ON aes.patient_id = ps.patient_id
    LEFT JOIN drug_silver ds
        ON aes.drug_id = ds.drug_id
    """
)
aggregated_dataset_silver_df.createOrReplaceTempView("aggregated_dataset_silver")

(
    aggregated_dataset_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_dataset_silver.csv")
)

job.commit()