import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read Source Tables (S3)
# ----------------------------
patient_analytics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_analytics_silver.{FILE_FORMAT}/")
)
patient_analytics_silver_df.createOrReplaceTempView("patient_analytics_silver")

patient_dashboard_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_dashboard_metrics_silver.{FILE_FORMAT}/")
)
patient_dashboard_metrics_silver_df.createOrReplaceTempView("patient_dashboard_metrics_silver")

report_latency_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/report_latency_silver.{FILE_FORMAT}/")
)
report_latency_silver_df.createOrReplaceTempView("report_latency_silver")

data_process_monitor_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_process_monitor_silver.{FILE_FORMAT}/")
)
data_process_monitor_silver_df.createOrReplaceTempView("data_process_monitor_silver")

# ----------------------------
# Target: gold.gold_patient_analytics
# ----------------------------
gold_patient_analytics_df = spark.sql(
    """
    SELECT
        pas.patient_id AS patient_id,
        pas.healthcare_system AS healthcare_system,
        pas.record_timestamp AS record_timestamp
    FROM patient_analytics_silver pas
    """
)

(
    gold_patient_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_analytics.csv")
)

# ----------------------------
# Target: gold.gold_clinical_dashboard
# ----------------------------
gold_clinical_dashboard_df = spark.sql(
    """
    SELECT
        pdms.patient_id AS patient_id
    FROM patient_dashboard_metrics_silver pdms
    LEFT JOIN report_latency_silver rls
        ON pdms.patient_id = rls.patient_id
       AND pdms.metric_as_of_timestamp = rls.metric_as_of_timestamp
    """
)

(
    gold_clinical_dashboard_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_dashboard.csv")
)

# ----------------------------
# Target: gold.gold_data_process_monitor
# ----------------------------
gold_data_process_monitor_df = spark.sql(
    """
    SELECT
        dpms.data_updated_timestamp AS data_updated_timestamp
    FROM data_process_monitor_silver dpms
    """
)

(
    gold_data_process_monitor_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_process_monitor.csv")
)
