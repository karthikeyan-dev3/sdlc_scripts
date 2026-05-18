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

# =========================
# Read Source Tables (S3)
# =========================
lab_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_silver.{FILE_FORMAT}/")
)
lab_results_silver_df.createOrReplaceTempView("lab_results_silver")

patient_lab_trend_inputs_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_lab_trend_inputs_silver.{FILE_FORMAT}/")
)
patient_lab_trend_inputs_silver_df.createOrReplaceTempView("patient_lab_trend_inputs_silver")

integrated_patient_snapshot_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/integrated_patient_snapshot_silver.{FILE_FORMAT}/")
)
integrated_patient_snapshot_silver_df.createOrReplaceTempView("integrated_patient_snapshot_silver")

patient_therapy_episodes_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_therapy_episodes_silver.{FILE_FORMAT}/")
)
patient_therapy_episodes_silver_df.createOrReplaceTempView("patient_therapy_episodes_silver")

patient_risk_features_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_risk_features_silver.{FILE_FORMAT}/")
)
patient_risk_features_silver_df.createOrReplaceTempView("patient_risk_features_silver")

# ==========================================================
# Target: gold_patient_lab_results (gplr)
# ==========================================================
gold_patient_lab_results_df = spark.sql(
    """
    SELECT
        CAST(lrs.patient_id AS STRING)            AS patient_id,
        CAST(lrs.lab_result_id AS STRING)         AS lab_result_id,
        CAST(lrs.test_date AS TIMESTAMP)          AS test_date,
        CAST(lrs.test_type AS STRING)             AS test_type,
        CAST(lrs.test_result_value AS DOUBLE)     AS test_result_value,
        CAST(lrs.result_unit AS STRING)           AS result_unit,
        CAST(lrs.abnormal_flag AS STRING)         AS abnormal_flag
    FROM lab_results_silver lrs
    """
)

(
    gold_patient_lab_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_lab_results.csv")
)

# ==========================================================
# Target: gold_patient_trends (gpt)
# ==========================================================
gold_patient_trends_df = spark.sql(
    """
    SELECT
        CAST(pltis.patient_id AS STRING) AS patient_id,
        CAST(MIN(CAST(pltis.test_date AS TIMESTAMP)) AS TIMESTAMP) AS trend_start_date,
        CAST(MAX(CAST(pltis.test_date AS TIMESTAMP)) AS TIMESTAMP) AS trend_end_date,
        CAST(pltis.test_type AS STRING) AS indicator,
        CAST(
            MAX(CAST(pltis.test_result_value AS DOUBLE)) - MIN(CAST(pltis.test_result_value AS DOUBLE))
            AS DOUBLE
        ) AS trend_value_change,
        CAST(
            CASE
                WHEN (MAX(CAST(pltis.test_result_value AS DOUBLE)) - MIN(CAST(pltis.test_result_value AS DOUBLE))) > 0
                    THEN 'increasing'
                WHEN (MAX(CAST(pltis.test_result_value AS DOUBLE)) - MIN(CAST(pltis.test_result_value AS DOUBLE))) < 0
                    THEN 'decreasing'
                ELSE 'stable'
            END
            AS STRING
        ) AS trend_type,
        CAST(
            CASE
                WHEN SUM(CASE WHEN pltis.abnormal_flag = 'Y' THEN 1 ELSE 0 END) > 0
                    THEN 'high'
                ELSE 'low'
            END
            AS STRING
        ) AS risk_category
    FROM patient_lab_trend_inputs_silver pltis
    GROUP BY
        pltis.patient_id,
        pltis.test_type
    """
)

(
    gold_patient_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_trends.csv")
)

# ==========================================================
# Target: gold_abnormal_results_analysis (gara)
# ==========================================================
gold_abnormal_results_analysis_df = spark.sql(
    """
    SELECT
        CAST(pltis.patient_id AS STRING) AS patient_id,
        CAST(pltis.test_type AS STRING)  AS test_type,
        CAST(
            SUM(CASE WHEN pltis.abnormal_flag = 'Y' THEN 1 ELSE 0 END)
            AS INT
        ) AS abnormal_result_count,
        CAST(MIN(CAST(pltis.test_date AS TIMESTAMP)) AS TIMESTAMP) AS observation_period_start,
        CAST(MAX(CAST(pltis.test_date AS TIMESTAMP)) AS TIMESTAMP) AS observation_period_end
    FROM patient_lab_trend_inputs_silver pltis
    GROUP BY
        pltis.patient_id,
        pltis.test_type
    """
)

(
    gold_abnormal_results_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_abnormal_results_analysis.csv")
)

# ==========================================================
# Target: gold_integrated_patient_data (gipd)
# ==========================================================
gold_integrated_patient_data_df = spark.sql(
    """
    SELECT
        CAST(ipss.patient_id AS STRING)              AS patient_id,
        CAST(ipss.demographics AS STRING)            AS demographics,
        CAST(ipss.health_history AS STRING)          AS health_history,
        CAST(ipss.wearable_data_summary AS STRING)   AS wearable_data_summary,
        CAST(ipss.combined_lab_results AS STRING)    AS combined_lab_results
    FROM integrated_patient_snapshot_silver ipss
    """
)

(
    gold_integrated_patient_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_integrated_patient_data.csv")
)

# ==========================================================
# Target: gold_drug_effectiveness (gde)
# ==========================================================
gold_drug_effectiveness_df = spark.sql(
    """
    SELECT
        CAST(ptes.patient_id AS STRING) AS patient_id,
        CAST(ptes.medication_name AS STRING) AS medication_name,
        CAST(ptes.treatment_start_date AS TIMESTAMP) AS treatment_start_date,
        CAST(ptes.treatment_end_date AS TIMESTAMP) AS treatment_end_date,
        CAST(
            MAX_BY(CAST(pltis.test_result_value AS DOUBLE), CAST(pltis.test_date AS TIMESTAMP))
                FILTER (WHERE CAST(pltis.test_date AS TIMESTAMP) < CAST(ptes.treatment_start_date AS TIMESTAMP))
            AS DOUBLE
        ) AS pre_treatment_indicator_value,
        CAST(
            MAX_BY(CAST(pltis.test_result_value AS DOUBLE), CAST(pltis.test_date AS TIMESTAMP))
                FILTER (WHERE CAST(pltis.test_date AS TIMESTAMP) > CAST(ptes.treatment_end_date AS TIMESTAMP))
            AS DOUBLE
        ) AS post_treatment_indicator_value,
        CAST(
            (
                MAX_BY(CAST(pltis.test_result_value AS DOUBLE), CAST(pltis.test_date AS TIMESTAMP))
                    FILTER (WHERE CAST(pltis.test_date AS TIMESTAMP) > CAST(ptes.treatment_end_date AS TIMESTAMP))
            ) - (
                MAX_BY(CAST(pltis.test_result_value AS DOUBLE), CAST(pltis.test_date AS TIMESTAMP))
                    FILTER (WHERE CAST(pltis.test_date AS TIMESTAMP) < CAST(ptes.treatment_start_date AS TIMESTAMP))
            )
            AS DOUBLE
        ) AS effectiveness_measure
    FROM patient_therapy_episodes_silver ptes
    LEFT JOIN patient_lab_trend_inputs_silver pltis
        ON ptes.patient_id = pltis.patient_id
    GROUP BY
        ptes.patient_id,
        ptes.medication_name,
        ptes.treatment_start_date,
        ptes.treatment_end_date
    """
)

(
    gold_drug_effectiveness_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_drug_effectiveness.csv")
)

# ==========================================================
# Target: gold_high_risk_patients (ghrp)
# ==========================================================
gold_high_risk_patients_df = spark.sql(
    """
    SELECT
        CAST(prfs.patient_id AS STRING) AS patient_id,
        CAST(prfs.feature_as_of_date AS DATE) AS risk_assessment_date
    FROM patient_risk_features_silver prfs
    """
)

(
    gold_high_risk_patients_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_high_risk_patients.csv")
)

job.commit()
