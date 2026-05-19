import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# Read source tables from S3 + create temp views
# -------------------------------------------------------------------

clinical_trials_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trials_silver.{FILE_FORMAT}/")
)
clinical_trials_silver_df.createOrReplaceTempView("clinical_trials_silver")

audit_trail_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/audit_trail_silver.{FILE_FORMAT}/")
)
audit_trail_silver_df.createOrReplaceTempView("audit_trail_silver")

data_lineage_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_lineage_silver.{FILE_FORMAT}/")
)
data_lineage_silver_df.createOrReplaceTempView("data_lineage_silver")

historical_versions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/historical_versions_silver.{FILE_FORMAT}/")
)
historical_versions_silver_df.createOrReplaceTempView("historical_versions_silver")

# -------------------------------------------------------------------
# Target: gold_clinical_trials
# -------------------------------------------------------------------

gold_clinical_trials_df = spark.sql(
    """
    SELECT
        cts.trial_id AS trial_id,
        CAST(cts.start_date AS DATE) AS start_date,
        cts.trial_name AS trial_name,
        cts.study_phase AS study_phase,
        cts.end_date AS end_date,
        cts.principal_investigator AS principal_investigator,
        cts.sponsor AS sponsor,
        cts.compliance_status AS compliance_status
    FROM clinical_trials_silver cts
    """
)

(
    gold_clinical_trials_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trials.csv")
)

# -------------------------------------------------------------------
# Target: gold_audit_trail
# -------------------------------------------------------------------

gold_audit_trail_df = spark.sql(
    """
    SELECT
        ats.record_id AS record_id,
        CAST(ats.change_timestamp AS TIMESTAMP) AS change_timestamp,
        ats.changed_by AS changed_by,
        ats.field_name AS field_name,
        ats.old_value AS old_value,
        ats.new_value AS new_value,
        ats.change_reason AS change_reason
    FROM audit_trail_silver ats
    """
)

(
    gold_audit_trail_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_audit_trail.csv")
)

# -------------------------------------------------------------------
# Target: gold_data_lineage
# -------------------------------------------------------------------

gold_data_lineage_df = spark.sql(
    """
    SELECT
        dls.lineage_id AS lineage_id,
        dls.source_system AS source_system,
        dls.transformation_description AS transformation_description,
        dls.target_table AS target_table,
        dls.load_timestamp AS load_timestamp
    FROM data_lineage_silver dls
    """
)

(
    gold_data_lineage_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_lineage.csv")
)

# -------------------------------------------------------------------
# Target: gold_historical_versions
# -------------------------------------------------------------------

gold_historical_versions_df = spark.sql(
    """
    SELECT
        hvs.version_id AS version_id,
        hvs.record_id AS record_id,
        hvs.version_number AS version_number,
        CAST(hvs.version_start_date AS DATE) AS version_start_date,
        hvs.version_end_date AS version_end_date,
        hvs.data_snapshot AS data_snapshot
    FROM historical_versions_silver hvs
    """
)

(
    gold_historical_versions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_historical_versions.csv")
)

job.commit()