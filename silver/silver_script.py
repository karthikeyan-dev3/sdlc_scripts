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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# Read Source Tables (Bronze)
# --------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# --------------------------------------------------------------------
# TARGET: silver_trial_enrollment_statistics
# --------------------------------------------------------------------
silver_trial_enrollment_statistics_df = spark.sql(
    """
    WITH base AS (
        SELECT
            pb.category AS trial_id,
            sb.state AS country,
            sb.store_id AS site_id,
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(pb.is_active AS BOOLEAN) AS is_active
        FROM sales_transactions_bronze stb
        INNER JOIN stores_bronze sb
            ON stb.store_id = sb.store_id
        INNER JOIN products_bronze pb
            ON stb.product_id = pb.product_id
    ),
    dedup AS (
        SELECT
            trial_id,
            country,
            site_id,
            transaction_id,
            transaction_time,
            quantity,
            is_active
        FROM (
            SELECT
                trial_id,
                country,
                site_id,
                transaction_id,
                transaction_time,
                quantity,
                is_active,
                ROW_NUMBER() OVER (
                    PARTITION BY transaction_id
                    ORDER BY transaction_time DESC
                ) AS rn
            FROM base
        ) t
        WHERE rn = 1
    )
    SELECT
        trial_id,
        country,
        site_id,
        CAST(SUM(CASE WHEN is_active = true THEN COALESCE(quantity, 0) ELSE 0 END) AS INT) AS enrolling_patients_count,
        CAST(SUM(CASE WHEN is_active = false THEN COALESCE(quantity, 0) ELSE 0 END) AS INT) AS dropout_patients_count,
        CAST(COUNT(DISTINCT transaction_id) AS INT) AS visits_completed_count,
        CAST(
            SUM(COALESCE(quantity, 0)) / NULLIF(COUNT(DISTINCT DATE(transaction_time)), 0)
            AS DOUBLE
        ) AS enrollment_rate
    FROM dedup
    GROUP BY
        trial_id,
        country,
        site_id
    """
)
silver_trial_enrollment_statistics_df.createOrReplaceTempView("silver_trial_enrollment_statistics")

(
    silver_trial_enrollment_statistics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_trial_enrollment_statistics.csv")
)

# --------------------------------------------------------------------
# TARGET: silver_site_performance_metrics
# --------------------------------------------------------------------
silver_site_performance_metrics_df = spark.sql(
    """
    WITH base AS (
        SELECT
            trial_id,
            country,
            site_id,
            CAST(enrolling_patients_count AS DOUBLE) AS enrolling_patients_count,
            CAST(dropout_patients_count AS DOUBLE) AS dropout_patients_count,
            CAST(visits_completed_count AS DOUBLE) AS visits_completed_count
        FROM silver_trial_enrollment_statistics
    ),
    stats AS (
        SELECT
            trial_id,
            country,
            site_id,
            enrolling_patients_count,
            dropout_patients_count,
            visits_completed_count,
            AVG(enrolling_patients_count) OVER (PARTITION BY trial_id) AS avg_enroll,
            STDDEV_POP(enrolling_patients_count) OVER (PARTITION BY trial_id) AS std_enroll
        FROM base
    )
    SELECT
        trial_id,
        country,
        site_id,
        CAST(
            (enrolling_patients_count - avg_enroll) / NULLIF(std_enroll, 0)
            AS DOUBLE
        ) AS underperformance_metric,
        CAST(
            -(enrolling_patients_count - COALESCE(LAG(enrolling_patients_count, 1) OVER (
                PARTITION BY trial_id, country, site_id
                ORDER BY trial_id
            ), enrolling_patients_count))
            AS DOUBLE
        ) AS enrollment_delay_metric,
        CAST(
            1 - (dropout_patients_count / NULLIF(enrolling_patients_count, 0))
            AS DOUBLE
        ) AS retention_rate_metric,
        CAST(
            visits_completed_count / NULLIF(enrolling_patients_count, 0)
            AS DOUBLE
        ) AS adherence_rate_metric
    FROM stats
    """
)

(
    silver_site_performance_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_site_performance_metrics.csv")
)

# --------------------------------------------------------------------
# TARGET: silver_report_generation_logs
# --------------------------------------------------------------------
silver_report_generation_logs_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS report_id,
            pb.category AS trial_id,
            sb.state AS country,
            sb.store_id AS site_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
        FROM sales_transactions_bronze stb
        INNER JOIN stores_bronze sb
            ON stb.store_id = sb.store_id
        INNER JOIN products_bronze pb
            ON stb.product_id = pb.product_id
    ),
    agg AS (
        SELECT
            report_id,
            trial_id,
            country,
            site_id,
            MIN(transaction_time) AS initial_request_time,
            MAX(transaction_time) AS report_generated_time
        FROM base
        GROUP BY
            report_id,
            trial_id,
            country,
            site_id
    ),
    dedup AS (
        SELECT
            report_id,
            trial_id,
            country,
            site_id,
            initial_request_time,
            report_generated_time
        FROM (
            SELECT
                report_id,
                trial_id,
                country,
                site_id,
                initial_request_time,
                report_generated_time,
                ROW_NUMBER() OVER (
                    PARTITION BY report_id
                    ORDER BY report_generated_time DESC
                ) AS rn
            FROM agg
        ) t
        WHERE rn = 1
    )
    SELECT
        report_id,
        trial_id,
        country,
        site_id,
        initial_request_time,
        report_generated_time,
        CAST(
            (UNIX_TIMESTAMP(report_generated_time) - UNIX_TIMESTAMP(initial_request_time))
            AS INT
        ) AS latency_metric
    FROM dedup
    """
)

(
    silver_report_generation_logs_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_report_generation_logs.csv")
)

job.commit()