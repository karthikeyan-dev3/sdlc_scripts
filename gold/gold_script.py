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

# ==============================
# 1) Read source tables from S3
# ==============================
trials_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trials_silver.{FILE_FORMAT}/")
)

patient_enrollments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollments_silver.{FILE_FORMAT}/")
)

patient_visits_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_visits_silver.{FILE_FORMAT}/")
)

sites_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sites_silver.{FILE_FORMAT}/")
)

# ==============================
# 2) Create temp views
# ==============================
trials_silver_df.createOrReplaceTempView("trials_silver")
patient_enrollments_silver_df.createOrReplaceTempView("patient_enrollments_silver")
patient_visits_silver_df.createOrReplaceTempView("patient_visits_silver")
sites_silver_df.createOrReplaceTempView("sites_silver")

# =========================================================
# Target: gold.gold_trial_performance (gtp)
# =========================================================
gold_trial_performance_df = spark.sql(
    """
    SELECT
        ts.trial_id AS trial_id,
        ts.trial_name AS trial_name,
        COUNT(DISTINCT pes.patient_id) AS total_enrolled_patients,
        COUNT(DISTINCT CASE WHEN pes.patient_status = 'ACTIVE' THEN pes.patient_id END) AS active_patients,
        COUNT(DISTINCT CASE WHEN pes.patient_status = 'DROPPED' THEN pes.patient_id END) AS dropout_count,
        CAST(COUNT(pes.enrollment_date) AS varchar(255)) AS enrollment_trend,
        CAST(
            100.00 * COUNT(DISTINCT pvs.visit_id) / NULLIF(COUNT(DISTINCT pvs.visit_id), 0)
            AS decimal(5,2)
        ) AS visit_completion_percentage
    FROM trials_silver ts
    LEFT JOIN patient_enrollments_silver pes
        ON ts.trial_id = pes.trial_id
    LEFT JOIN patient_visits_silver pvs
        ON ts.trial_id = pvs.trial_id
    GROUP BY
        ts.trial_id,
        ts.trial_name
    """
)

(
    gold_trial_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_trial_performance.csv")
)

# =========================================================
# Target: gold.gold_country_performance (gcp)
# =========================================================
gold_country_performance_df = spark.sql(
    """
    SELECT
        pes.trial_id AS trial_id,
        pes.country AS country,
        COUNT(DISTINCT pes.patient_id) AS total_enrolled_patients,
        CAST(
            1.0 * COUNT(DISTINCT CASE WHEN pes.patient_status <> 'DROPPED' THEN pes.patient_id END)
            / NULLIF(COUNT(DISTINCT pes.patient_id), 0)
            AS decimal(6,4)
        ) AS patient_retention_rate,
        CAST(COUNT(DISTINCT pvs.visit_id) AS decimal(10,4)) AS site_performance_score,
        COUNT(DISTINCT ss.site_id) AS underperforming_sites
    FROM patient_enrollments_silver pes
    LEFT JOIN patient_visits_silver pvs
        ON pes.trial_id = pvs.trial_id
       AND pes.country = pvs.country
    LEFT JOIN sites_silver ss
        ON pes.trial_id = ss.trial_id
       AND pes.site_id = ss.site_id
    GROUP BY
        pes.trial_id,
        pes.country
    """
)

(
    gold_country_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_country_performance.csv")
)

# =========================================================
# Target: gold.gold_site_performance (gsp)
# =========================================================
gold_site_performance_df = spark.sql(
    """
    SELECT
        ss.site_id AS site_id,
        ss.trial_id AS trial_id,
        ss.country AS country,
        COUNT(DISTINCT pes.patient_id) AS total_enrolled_patients,
        CAST(
            1.0 * COUNT(DISTINCT CASE WHEN pes.patient_status <> 'DROPPED' THEN pes.patient_id END)
            / NULLIF(COUNT(DISTINCT pes.patient_id), 0)
            AS decimal(6,4)
        ) AS patient_retention_rate,
        CAST(
            1.0 * COUNT(DISTINCT pvs.visit_id) / NULLIF(COUNT(DISTINCT pvs.visit_id), 0)
            AS decimal(6,4)
        ) AS visit_adherence_rate
    FROM sites_silver ss
    LEFT JOIN patient_enrollments_silver pes
        ON ss.trial_id = pes.trial_id
       AND ss.site_id = pes.site_id
    LEFT JOIN patient_visits_silver pvs
        ON ss.trial_id = pvs.trial_id
       AND ss.site_id = pvs.site_id
    GROUP BY
        ss.site_id,
        ss.trial_id,
        ss.country
    """
)

(
    gold_site_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_site_performance.csv")
)

job.commit()