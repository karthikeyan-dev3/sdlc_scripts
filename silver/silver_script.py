import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------
# Read Source Tables (Bronze)
# -------------------------
clinical_trials_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trials_bronze.{FILE_FORMAT}/")
)
sites_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sites_bronze.{FILE_FORMAT}/")
)
enrollments_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/enrollments_bronze.{FILE_FORMAT}/")
)
visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/visits_bronze.{FILE_FORMAT}/")
)
site_enrollment_targets_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/site_enrollment_targets_bronze.{FILE_FORMAT}/")
)
pipeline_run_logs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/pipeline_run_logs_bronze.{FILE_FORMAT}/")
)
source_ingestion_audit_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/source_ingestion_audit_bronze.{FILE_FORMAT}/")
)

clinical_trials_bronze_df.createOrReplaceTempView("clinical_trials_bronze")
sites_bronze_df.createOrReplaceTempView("sites_bronze")
enrollments_bronze_df.createOrReplaceTempView("enrollments_bronze")
visits_bronze_df.createOrReplaceTempView("visits_bronze")
site_enrollment_targets_bronze_df.createOrReplaceTempView("site_enrollment_targets_bronze")
pipeline_run_logs_bronze_df.createOrReplaceTempView("pipeline_run_logs_bronze")
source_ingestion_audit_bronze_df.createOrReplaceTempView("source_ingestion_audit_bronze")

# -------------------------
# Target: clinical_trial_sites_silver
# -------------------------
clinical_trial_sites_silver_df = spark.sql(
    """
    SELECT
        ct.trial_id AS trial_id,
        s.site_id AS site_id,
        s.country AS country,
        CAST(v.visit_date AS DATE) AS metric_date,
        CAST(COUNT(DISTINCT e.patient_id) AS INT) AS total_enrolled_patients,
        CAST(
            SUM(CASE WHEN v.visit_status = 'COMPLETED' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(1), 0)
            AS DOUBLE
        ) AS visit_completion_rate
    FROM clinical_trials_bronze ct
    INNER JOIN sites_bronze s
        ON ct.trial_id = s.trial_id
    LEFT JOIN enrollments_bronze e
        ON e.trial_id = ct.trial_id
       AND e.site_id = s.site_id
    LEFT JOIN visits_bronze v
        ON v.trial_id = ct.trial_id
       AND v.site_id = s.site_id
    GROUP BY
        ct.trial_id,
        s.site_id,
        s.country,
        CAST(v.visit_date AS DATE)
    """
)
clinical_trial_sites_silver_df.createOrReplaceTempView("clinical_trial_sites_silver")

(
    clinical_trial_sites_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_sites_silver.csv")
)

# -------------------------
# Target: trial_enrollment_analysis_silver
# -------------------------
trial_enrollment_analysis_silver_df = spark.sql(
    """
    SELECT
        ctss.trial_id AS trial_id,
        ctss.site_id AS site_id,
        CAST(ctss.metric_date AS DATE) AS metric_date,
        CAST(ctss.total_enrolled_patients AS INT) AS enrollment_numbers,
        CAST(setb.standard_enrollment_rate AS DOUBLE) AS standard_enrollment_rate,
        CAST(
            CASE
                WHEN ctss.total_enrolled_patients < setb.standard_enrollment_rate THEN 1
                ELSE 0
            END AS INT
        ) AS enrollment_delay_flag
    FROM clinical_trial_sites_silver ctss
    LEFT JOIN site_enrollment_targets_bronze setb
        ON ctss.trial_id = setb.trial_id
       AND ctss.site_id = setb.site_id
    """
)
trial_enrollment_analysis_silver_df.createOrReplaceTempView("trial_enrollment_analysis_silver")

(
    trial_enrollment_analysis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_enrollment_analysis_silver.csv")
)

# -------------------------
# Target: performance_trends_silver
# -------------------------
performance_trends_silver_df = spark.sql(
    """
    SELECT
        ctss.trial_id AS trial_id,
        ctss.site_id AS site_id,
        CAST(ctss.metric_date AS DATE) AS analysis_date,
        to_json(
            named_struct(
                'total_enrolled_patients', ctss.total_enrolled_patients,
                'visit_completion_rate', ctss.visit_completion_rate,
                'enrollment_delay_flag', teas.enrollment_delay_flag
            )
        ) AS historical_metrics,
        to_json(
            named_struct(
                'metric_date', ctss.metric_date,
                'total_enrolled_patients', ctss.total_enrolled_patients,
                'visit_completion_rate', ctss.visit_completion_rate,
                'enrollment_delay_flag', teas.enrollment_delay_flag
            )
        ) AS trend_data
    FROM clinical_trial_sites_silver ctss
    LEFT JOIN trial_enrollment_analysis_silver teas
        ON ctss.trial_id = teas.trial_id
       AND ctss.site_id = teas.site_id
       AND ctss.metric_date = teas.metric_date
    """
)
performance_trends_silver_df.createOrReplaceTempView("performance_trends_silver")

(
    performance_trends_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/performance_trends_silver.csv")
)

# -------------------------
# Target: data_quality_checks_silver
# -------------------------
data_quality_checks_silver_df = spark.sql(
    """
    SELECT
        CAST(prl.data_quality_score AS DOUBLE) AS data_quality_score,
        CAST(sia.reporting_latency AS DOUBLE) AS reporting_latency,
        CAST(sia.data_freshness_percentage AS DOUBLE) AS data_freshness_percentage,
        CAST(prl.processing_time AS DOUBLE) AS processing_time,
        CAST(prl.last_update_timestamp AS TIMESTAMP) AS last_update_timestamp,
        CAST(prl.evaluation_date AS DATE) AS evaluation_date
    FROM pipeline_run_logs_bronze prl
    LEFT JOIN source_ingestion_audit_bronze sia
        ON prl.pipeline_id = sia.pipeline_id
       AND prl.run_id = sia.run_id
    """
)
data_quality_checks_silver_df.createOrReplaceTempView("data_quality_checks_silver")

(
    data_quality_checks_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_checks_silver.csv")
)

job.commit()
