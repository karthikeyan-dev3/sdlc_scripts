import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
# ---------------------------------------------------------------------
patient_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_silver.{FILE_FORMAT}/")
)

drug_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_silver.{FILE_FORMAT}/")
)

site_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_silver.{FILE_FORMAT}/")
)

adverse_event_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_event_silver.{FILE_FORMAT}/")
)

aggregated_dataset_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_dataset_silver.{FILE_FORMAT}/")
)

# ---------------------------------------------------------------------
# 2) Create temp views
# ---------------------------------------------------------------------
patient_silver_df.createOrReplaceTempView("patient_silver")
drug_silver_df.createOrReplaceTempView("drug_silver")
site_silver_df.createOrReplaceTempView("site_silver")
adverse_event_silver_df.createOrReplaceTempView("adverse_event_silver")
aggregated_dataset_silver_df.createOrReplaceTempView("aggregated_dataset_silver")

# ---------------------------------------------------------------------
# 3) Transform + 4) Save outputs (each target table separately)
# ---------------------------------------------------------------------

# =========================
# gold.patient_master (gpm)
# =========================
patient_master_df = spark.sql(
    """
    SELECT
        CAST(ps.patient_id AS STRING) AS patient_id,
        CAST(ps.age AS INT) AS age,
        CAST(ps.gender AS STRING) AS gender,
        CAST(ps.enrollment_date AS DATE) AS enrollment_date
    FROM patient_silver ps
    """
)

(
    patient_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/patient_master.csv")
)

# ======================
# gold.drug_master (gdm)
# ======================
drug_master_df = spark.sql(
    """
    SELECT
        CAST(ds.drug_id AS STRING) AS drug_id,
        CAST(ds.drug_name AS STRING) AS drug_name,
        CAST(ds.drug_category AS STRING) AS drug_category,
        CAST(ds.manufacturer AS STRING) AS manufacturer
    FROM drug_silver ds
    """
)

(
    drug_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/drug_master.csv")
)

# ======================
# gold.site_master (gsm)
# ======================
site_master_df = spark.sql(
    """
    SELECT
        CAST(ss.site_id AS STRING) AS site_id,
        CAST(ss.site_location AS STRING) AS site_location
    FROM site_silver ss
    """
)

(
    site_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/site_master.csv")
)

# ==================================
# gold.adverse_event_analytics (gaea)
# ==================================
adverse_event_analytics_df = spark.sql(
    """
    SELECT
        CAST(aes.ae_id AS STRING) AS adverse_event_id,
        CAST(aes.patient_id AS STRING) AS patient_id,
        CAST(aes.drug_id AS STRING) AS drug_id,
        CAST(aes.site_id AS STRING) AS site_id,
        CAST(aes.event_date AS DATE) AS event_date,
        CAST(aes.event_type AS STRING) AS event_type,
        CAST(aes.severity AS STRING) AS severity
    FROM adverse_event_silver aes
    LEFT JOIN patient_silver ps
        ON aes.patient_id = ps.patient_id
    LEFT JOIN drug_silver ds
        ON aes.drug_id = ds.drug_id
    LEFT JOIN site_silver ss
        ON aes.site_id = ss.site_id
    """
)

(
    adverse_event_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/adverse_event_analytics.csv")
)

# =================================
# gold.aggregated_datasets (gad)
# =================================
aggregated_datasets_df = spark.sql(
    """
    SELECT
        CAST(ads.report_date AS DATE) AS report_date
    FROM aggregated_dataset_silver ads
    """
)

(
    aggregated_datasets_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/aggregated_datasets.csv")
)

job.commit()
