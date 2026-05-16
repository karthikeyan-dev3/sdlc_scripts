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

# ------------------------------------------------------------
# SOURCE: silver.adverse_events_silver (aes)
# TARGET: gold.gold_adverse_events
# ------------------------------------------------------------
aes_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_silver.{FILE_FORMAT}/")
)
aes_df.createOrReplaceTempView("adverse_events_silver")

gold_adverse_events_df = spark.sql(
    """
    SELECT
        CAST(aes.adverse_event_id AS STRING) AS adverse_event_id,
        CAST(aes.description AS STRING) AS description,
        CAST(aes.severity_classification AS STRING) AS severity_classification
    FROM adverse_events_silver aes
    """
)

(
    gold_adverse_events_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_adverse_events.csv")
)

# ------------------------------------------------------------
# SOURCE: silver.drug_safety_analysis_silver (dsas)
# TARGET: gold.gold_drug_safety
# ------------------------------------------------------------
dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_safety_analysis_silver.{FILE_FORMAT}/")
)
dsas_df.createOrReplaceTempView("drug_safety_analysis_silver")

gold_drug_safety_df = spark.sql(
    """
    SELECT
        CAST(dsas.patient_id AS STRING) AS patient_id,
        CAST(dsas.drug_id AS STRING) AS drug_id,
        CAST(dsas.dosage_amount AS DOUBLE) AS dosage_amount,
        CAST(dsas.adverse_event_id AS STRING) AS adverse_event_id,
        CAST(dsas.event_severity AS STRING) AS event_severity,
        CAST(dsas.hospitalization_indicator AS BOOLEAN) AS hospitalization_indicator,
        CAST(dsas.sae_count AS INT) AS sae_count,
        CAST(dsas.drug_event_correlation AS BOOLEAN) AS drug_event_correlation,
        CAST(dsas.high_risk_patient_indicator AS BOOLEAN) AS high_risk_patient_indicator
    FROM drug_safety_analysis_silver dsas
    """
)

(
    gold_drug_safety_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_safety.csv")
)

job.commit()