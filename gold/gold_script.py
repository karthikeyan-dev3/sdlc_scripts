import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read Source Tables (S3)
# -----------------------------------------------------------------------------------
wearable_health_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_health_metrics_silver.{FILE_FORMAT}/")
)
patients_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patients_silver.{FILE_FORMAT}/")
)
health_risk_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/health_risk_events_silver.{FILE_FORMAT}/")
)
clinical_intervention_insights_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_intervention_insights_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create Temp Views
# -----------------------------------------------------------------------------------
wearable_health_metrics_silver_df.createOrReplaceTempView("wearable_health_metrics_silver")
patients_silver_df.createOrReplaceTempView("patients_silver")
health_risk_events_silver_df.createOrReplaceTempView("health_risk_events_silver")
clinical_intervention_insights_silver_df.createOrReplaceTempView("clinical_intervention_insights_silver")

# -----------------------------------------------------------------------------------
# Target: gold.gold_wearable_health_monitoring
# Mapping: silver.wearable_health_metrics_silver whms INNER JOIN silver.patients_silver ps ON ps.patient_id = whms.patient_id
# -----------------------------------------------------------------------------------
gold_wearable_health_monitoring_df = spark.sql(
    """
    SELECT
        whms.patient_id AS patient_id,
        whms.timestamp AS timestamp,
        whms.glucose_level AS glucose_level,
        whms.heart_rate AS heart_rate,
        whms.sleep_duration AS sleep_duration,
        whms.physical_activity AS physical_activity
    FROM wearable_health_metrics_silver whms
    INNER JOIN patients_silver ps
        ON ps.patient_id = whms.patient_id
    """
)

(
    gold_wearable_health_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_wearable_health_monitoring.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_health_risk_identification
# Mapping: silver.health_risk_events_silver hres INNER JOIN silver.patients_silver ps ON ps.patient_id = hres.patient_id
# -----------------------------------------------------------------------------------
gold_health_risk_identification_df = spark.sql(
    """
    SELECT
        hres.patient_id AS patient_id,
        hres.health_risk_type AS health_risk_type,
        hres.risk_identified_at AS risk_identified_at,
        hres.risk_level AS risk_level
    FROM health_risk_events_silver hres
    INNER JOIN patients_silver ps
        ON ps.patient_id = hres.patient_id
    """
)

(
    gold_health_risk_identification_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_health_risk_identification.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_clinical_intervention_insights
# Mapping: silver.clinical_intervention_insights_silver ciis INNER JOIN silver.patients_silver ps ON ps.patient_id = ciis.patient_id
# -----------------------------------------------------------------------------------
gold_clinical_intervention_insights_df = spark.sql(
    """
    SELECT
        ciis.patient_id AS patient_id,
        ciis.insight_type AS insight_type,
        ciis.generated_at AS generated_at
    FROM clinical_intervention_insights_silver ciis
    INNER JOIN patients_silver ps
        ON ps.patient_id = ciis.patient_id
    """
)

(
    gold_clinical_intervention_insights_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_intervention_insights.csv")
)

job.commit()