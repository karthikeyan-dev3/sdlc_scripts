import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# Read Source Tables from S3 (CSV) and Create Temp Views
# -------------------------------------------------------------------

patient_daily_health_indicators_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_daily_health_indicators_silver.{FILE_FORMAT}/")
)
patient_daily_health_indicators_silver_df.createOrReplaceTempView("patient_daily_health_indicators_silver")

patient_response_features_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_response_features_silver.{FILE_FORMAT}/")
)
patient_response_features_silver_df.createOrReplaceTempView("patient_response_features_silver")

abnormal_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/abnormal_results_silver.{FILE_FORMAT}/")
)
abnormal_results_silver_df.createOrReplaceTempView("abnormal_results_silver")

integrated_health_data_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/integrated_health_data_silver.{FILE_FORMAT}/")
)
integrated_health_data_silver_df.createOrReplaceTempView("integrated_health_data_silver")

# -------------------------------------------------------------------
# Target: gold_patient_health_indicators
# -------------------------------------------------------------------

gold_patient_health_indicators_df = spark.sql(
    """
    SELECT
        CAST(pdhis.patient_id AS STRING) AS patient_id,
        DATE(pdhis.date) AS date,
        CAST(pdhis.hbA1c_level AS DOUBLE) AS hbA1c_level,
        CAST(pdhis.glucose_level AS DOUBLE) AS glucose_level
    FROM patient_daily_health_indicators_silver pdhis
    """
)

(
    gold_patient_health_indicators_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_health_indicators.csv")
)

# -------------------------------------------------------------------
# Target: gold_patient_response_analysis
# -------------------------------------------------------------------

gold_patient_response_analysis_df = spark.sql(
    """
    SELECT
        CAST(prfs.patient_id AS STRING) AS patient_id,
        DATE(prfs.date) AS date,
        CAST(prfs.response_pattern AS STRING) AS response_pattern,
        CAST(prfs.health_condition_change AS STRING) AS health_condition_change
    FROM patient_response_features_silver prfs
    """
)

(
    gold_patient_response_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_response_analysis.csv")
)

# -------------------------------------------------------------------
# Target: gold_abnormal_result_analysis
# -------------------------------------------------------------------

gold_abnormal_result_analysis_df = spark.sql(
    """
    SELECT
        CAST(ars.patient_id AS STRING) AS patient_id,
        CAST(ars.abnormal_result_type AS STRING) AS abnormal_result_type,
        CAST(COUNT(ars.patient_id) AS INT) AS occurrence_count
    FROM abnormal_results_silver ars
    GROUP BY
        ars.patient_id,
        ars.abnormal_result_type
    """
)

(
    gold_abnormal_result_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_abnormal_result_analysis.csv")
)

# -------------------------------------------------------------------
# Target: gold_patient_risk_categorization
# -------------------------------------------------------------------

gold_patient_risk_categorization_df = spark.sql(
    """
    SELECT
        CAST(prfs.patient_id AS STRING) AS patient_id,
        CAST(prfs.trend_analysis AS STRING) AS trend_analysis,
        CAST(
            CASE
                WHEN prfs.trend_analysis = 'INDICATOR_TREND' THEN 'ELEVATED_RISK'
                ELSE 'UNKNOWN'
            END AS STRING
        ) AS risk_category
    FROM patient_response_features_silver prfs
    """
)

(
    gold_patient_risk_categorization_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_risk_categorization.csv")
)

# -------------------------------------------------------------------
# Target: gold_integrated_health_data
# -------------------------------------------------------------------

gold_integrated_health_data_df = spark.sql(
    """
    SELECT
        CAST(ihds.patient_id AS STRING) AS patient_id,
        DATE(ihds.date) AS date,
        ihds.laboratory_data AS laboratory_data,
        ihds.wearable_data AS wearable_data,
        ihds.patient_related_data AS patient_related_data
    FROM integrated_health_data_silver ihds
    """
)

(
    gold_integrated_health_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_integrated_health_data.csv")
)

job.commit()