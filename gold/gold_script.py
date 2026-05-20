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

# ----------------------------
# Source: silver.patient_analytics_silver (pas)
# Target: gold.gold_patient_analytics
# ----------------------------
pas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_analytics_silver.{FILE_FORMAT}/")
)
pas_df.createOrReplaceTempView("patient_analytics_silver")

gpa_df = spark.sql(
    """
    SELECT
        CAST(pas.patient_id AS STRING) AS patient_id,
        CAST(pas.enrollment_date AS TIMESTAMP) AS enrollment_date,
        CAST(pas.clinical_visit_date AS TIMESTAMP) AS clinical_visit_date,
        CAST(pas.lab_result AS STRING) AS lab_result,
        CAST(pas.treatment_effectiveness AS STRING) AS treatment_effectiveness,
        CAST(pas.health_progression AS STRING) AS health_progression,
        CAST(pas.safety_risk AS STRING) AS safety_risk,
        CAST(pas.standardized_patient_identifier AS STRING) AS standardized_patient_identifier
    FROM patient_analytics_silver pas
    """
)

(
    gpa_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_analytics.csv")
)

# ----------------------------
# Source: silver.dashboard_reporting_silver (drs)
# Target: gold.gold_clinical_dashboard
# ----------------------------
drs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dashboard_reporting_silver.{FILE_FORMAT}/")
)
drs_df.createOrReplaceTempView("dashboard_reporting_silver")

gcd_df = spark.sql(
    """
    SELECT
        CAST(drs.dashboard_id AS STRING) AS dashboard_id,
        CAST(drs.reporting_tool AS STRING) AS reporting_tool,
        CAST(drs.access_date AS TIMESTAMP) AS access_date,
        CAST(drs.user_id AS STRING) AS user_id,
        CAST(drs.decision_support AS STRING) AS decision_support
    FROM dashboard_reporting_silver drs
    """
)

(
    gcd_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_dashboard.csv")
)

# ----------------------------
# Source: silver.data_governance_silver (dgs)
# Target: gold.gold_data_quality
# ----------------------------
dgs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_governance_silver.{FILE_FORMAT}/")
)
dgs_df.createOrReplaceTempView("data_governance_silver")

gdq_df = spark.sql(
    """
    SELECT
        CAST(dgs.data_source AS STRING) AS data_source,
        CAST(dgs.quality_score AS DOUBLE) AS quality_score,
        CAST(dgs.validation_checks AS STRING) AS validation_checks,
        CAST(dgs.compliance_audit_date AS TIMESTAMP) AS compliance_audit_date
    FROM data_governance_silver dgs
    """
)

(
    gdq_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()