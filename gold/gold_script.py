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

# -------------------------
# Read source tables (S3)
# -------------------------
sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/site_performance_silver.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("site_performance_silver")

shs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/site_health_silver.{FILE_FORMAT}/")
)
shs_df.createOrReplaceTempView("site_health_silver")

dvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_validation_silver.{FILE_FORMAT}/")
)
dvs_df.createOrReplaceTempView("data_validation_silver")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/system_metrics_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("system_metrics_silver")

# -------------------------
# Target: gold_site_performance
# -------------------------
gold_site_performance_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sps.site_id) AS STRING)                       AS site_id,
            CAST(TRIM(sps.site_name) AS STRING)                     AS site_name,
            CAST(sps.actual_enrollment AS INT)                      AS actual_enrollment,
            CAST(sps.last_activity_ts AS TIMESTAMP)                 AS data_refresh_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sps.site_id)
                ORDER BY CAST(sps.last_activity_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM site_performance_silver sps
    )
    SELECT
        site_id,
        site_name,
        actual_enrollment,
        data_refresh_timestamp
    FROM base
    WHERE rn = 1
    """
)

(
    gold_site_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_site_performance.csv")
)

# -------------------------
# Target: gold_site_health_report
# -------------------------
gold_site_health_report_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(shs.site_id) AS STRING)                       AS site_id,
            CAST(shs.report_date AS DATE)                           AS report_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(shs.site_id), CAST(shs.report_date AS DATE)
                ORDER BY CAST(COALESCE(shs.pipeline_ts, shs.refresh_ts) AS TIMESTAMP) DESC
            ) AS rn
        FROM site_health_silver shs
    )
    SELECT
        site_id,
        report_date
    FROM base
    WHERE rn = 1
    """
)

(
    gold_site_health_report_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_site_health_report.csv")
)

# -------------------------
# Target: gold_data_validation
# -------------------------
gold_data_validation_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(dvs.record_id) AS STRING)                     AS record_id,
            CAST(TRIM(dvs.site_id) AS STRING)                       AS site_id,
            CAST(dvs.run_timestamp AS TIMESTAMP)                    AS run_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(dvs.record_id)
                ORDER BY CAST(dvs.run_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM data_validation_silver dvs
    )
    SELECT
        record_id,
        site_id,
        run_timestamp
    FROM base
    WHERE rn = 1
    """
)

(
    gold_data_validation_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_data_validation.csv")
)

# -------------------------
# Target: gold_system_performance
# -------------------------
gold_system_performance_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sms.batch_id) AS STRING)                      AS batch_id,
            CAST(sms.process_start_ts AS TIMESTAMP)                 AS process_start_ts,
            CAST(sms.process_end_ts AS TIMESTAMP)                   AS process_end_ts,
            CAST(sms.records_processed AS BIGINT)                   AS records_processed,
            CAST(TRIM(sms.completion_status) AS STRING)             AS completion_status,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sms.batch_id)
                ORDER BY CAST(sms.process_end_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM system_metrics_silver sms
    )
    SELECT
        batch_id,
        process_start_ts,
        process_end_ts,
        records_processed,
        completion_status
    FROM base
    WHERE rn = 1
    """
)

(
    gold_system_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_system_performance.csv")
)

job.commit()