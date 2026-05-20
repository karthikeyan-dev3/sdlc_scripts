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

# ----------------------------
# 1) Read source tables from S3
# ----------------------------
tscps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_country_patient_silver.{FILE_FORMAT}/")
)

tspvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_site_patient_visit_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
tscps_df.createOrReplaceTempView("trial_site_country_patient_silver")
tspvs_df.createOrReplaceTempView("trial_site_patient_visit_silver")

# ============================================================
# TARGET TABLE: gold_clinical_trial_metrics
# ============================================================
gold_clinical_trial_metrics_df = spark.sql(
    """
    SELECT
        tscps.trial_id AS trial_id,
        tscps.country AS country,
        tscps.site_id AS site_id,
        CAST(COUNT(DISTINCT tscps.patient_id) AS INT) AS total_enrolled_patients,
        CAST(COUNT(DISTINCT CASE WHEN tscps.consent_status = 'active' THEN tscps.patient_id END) AS INT) AS active_patients,
        CAST(COUNT(DISTINCT CASE WHEN tscps.consent_status = 'dropout' THEN tscps.patient_id END) AS INT) AS patient_dropout_count,
        CAST(
            (COUNT(DISTINCT CASE WHEN tspvs.visit_date IS NOT NULL THEN tspvs.visit_id END) * 100.0)
            / NULLIF(COUNT(DISTINCT tspvs.visit_id), 0)
            AS DECIMAL(38, 10)
        ) AS visit_completion_percent,
        DATE_TRUNC('day', tscps.enrollment_date) AS enrollment_trend_over_time
    FROM trial_site_country_patient_silver tscps
    LEFT JOIN trial_site_patient_visit_silver tspvs
        ON  tscps.trial_id = tspvs.trial_id
        AND tscps.site_id = tspvs.site_id
        AND tscps.country = tspvs.country
        AND tscps.patient_id = tspvs.patient_id
    GROUP BY
        tscps.trial_id,
        tscps.country,
        tscps.site_id,
        DATE_TRUNC('day', tscps.enrollment_date)
    """
)

(
    gold_clinical_trial_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_clinical_trial_metrics.csv")
)

# ============================================================
# TARGET TABLE: gold_performance_tracking
# ============================================================
gold_performance_tracking_df = spark.sql(
    """
    SELECT
        tscps.trial_id AS trial_id,
        tscps.country AS country,
        tscps.site_id AS site_id,
        CASE
            WHEN COUNT(CASE WHEN tscps.enrollment_date >= DATEADD(day, -30, CURRENT_TIMESTAMP) THEN 1 END) = 0
                THEN TRUE
            ELSE FALSE
        END AS underperforming_site_flag,
        CAST(COUNT(CASE WHEN tscps.enrollment_date < DATEADD(day, -30, CURRENT_TIMESTAMP) THEN 1 END) AS INT) AS enrollment_delays,
        CAST(
            (COUNT(DISTINCT CASE WHEN tscps.consent_status = 'active' THEN tscps.patient_id END) * 1.0)
            / NULLIF(COUNT(DISTINCT tscps.patient_id), 0)
            AS DECIMAL(38, 10)
        ) AS patient_retention_rate,
        CAST(
            (COUNT(DISTINCT CASE WHEN tspvs.visit_date IS NOT NULL THEN tspvs.visit_id END) * 1.0)
            / NULLIF(COUNT(DISTINCT tspvs.visit_id), 0)
            AS DECIMAL(38, 10)
        ) AS visit_adherence
    FROM trial_site_country_patient_silver tscps
    LEFT JOIN trial_site_patient_visit_silver tspvs
        ON  tscps.trial_id = tspvs.trial_id
        AND tscps.site_id = tspvs.site_id
        AND tscps.country = tspvs.country
        AND tscps.patient_id = tspvs.patient_id
    GROUP BY
        tscps.trial_id,
        tscps.country,
        tscps.site_id
    """
)

(
    gold_performance_tracking_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_performance_tracking.csv")
)

# ============================================================
# TARGET TABLE: gold_historical_trends
# ============================================================
gold_historical_trends_df = spark.sql(
    """
    SELECT
        tscps.trial_id AS trial_id,
        tscps.source_system AS metric_name,
        CAST(COUNT(DISTINCT tscps.patient_id) AS DECIMAL(38, 10)) AS metric_value,
        DATE_TRUNC('day', tscps.enrollment_date) AS date
    FROM trial_site_country_patient_silver tscps
    LEFT JOIN trial_site_patient_visit_silver tspvs
        ON  tscps.trial_id = tspvs.trial_id
        AND tscps.site_id = tspvs.site_id
        AND tscps.country = tspvs.country
        AND tscps.patient_id = tspvs.patient_id
    GROUP BY
        tscps.trial_id,
        tscps.source_system,
        DATE_TRUNC('day', tscps.enrollment_date)
    """
)

(
    gold_historical_trends_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_historical_trends.csv")
)

# ============================================================
# TARGET TABLE: gold_data_quality
# ============================================================
gold_data_quality_df = spark.sql(
    """
    SELECT
        tscps.source_system AS metric_id,
        CAST(
            (COUNT(CASE WHEN tscps.patient_id IS NOT NULL AND tspvs.visit_id IS NOT NULL THEN 1 END) * 100.0)
            / NULLIF(COUNT(*), 0)
            AS DECIMAL(38, 10)
        ) AS accuracy_percentage,
        GREATEST(MAX(tscps.enrollment_date), MAX(tspvs.visit_date)) AS last_updated_time
    FROM trial_site_country_patient_silver tscps
    LEFT JOIN trial_site_patient_visit_silver tspvs
        ON  tscps.trial_id = tspvs.trial_id
        AND tscps.patient_id = tspvs.patient_id
    GROUP BY
        tscps.source_system
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()
