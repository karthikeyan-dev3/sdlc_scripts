import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
store_day_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_day_summary_silver.{FILE_FORMAT}/")
)
store_comparison_period_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_comparison_period_silver.{FILE_FORMAT}/")
)
data_quality_checks_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_checks_silver.{FILE_FORMAT}/")
)
metadata_tracking_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/metadata_tracking_silver.{FILE_FORMAT}/")
)

# =========================
# Create Temp Views
# =========================
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
store_day_summary_silver_df.createOrReplaceTempView("store_day_summary_silver")
store_comparison_period_silver_df.createOrReplaceTempView("store_comparison_period_silver")
data_quality_checks_silver_df.createOrReplaceTempView("data_quality_checks_silver")
metadata_tracking_silver_df.createOrReplaceTempView("metadata_tracking_silver")

# ======================================================
# Target: gold_sales_performance (gsp)
# ======================================================
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)              AS transaction_id,
        CAST(sts.product_id AS STRING)                  AS product_id,
        CAST(sts.store_id AS STRING)                    AS store_id,
        CAST(sts.transaction_date AS DATE)              AS transaction_date,
        CAST(sts.revenue AS DOUBLE)                     AS revenue,
        CAST(sts.quantity AS INT)                       AS quantity,
        CAST(ps.category AS STRING)                     AS category,
        CAST(ps.product_name AS STRING)                 AS product_name,
        CAST(ss.store_name AS STRING)                   AS store_name
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ======================================================
# Target: gold_store_performance (gstorep)
# ======================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sdss.store_id AS STRING)                           AS store_id,
        CAST(ss.store_name AS STRING)                           AS store_name,
        CAST(sdss.total_revenue AS DOUBLE)                      AS total_revenue,
        CAST(sdss.total_transactions AS INT)                    AS total_transactions,
        CAST(sdss.total_quantity AS INT)                        AS total_quantity,
        CAST(scps.comparison_period_revenue AS DOUBLE)          AS comparison_period_revenue,
        CAST(scps.comparison_period_transactions AS INT)        AS comparison_period_transactions
    FROM store_day_summary_silver sdss
    LEFT JOIN store_comparison_period_silver scps
        ON sdss.store_id = scps.store_id
       AND sdss.transaction_date = scps.transaction_date
    LEFT JOIN stores_silver ss
        ON sdss.store_id = ss.store_id
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ======================================================
# Target: gold_product_performance (gpp)
# ======================================================
gold_product_performance_df = spark.sql(
    """
    WITH product_totals AS (
        SELECT
            CAST(ps.product_id AS STRING)          AS product_id,
            CAST(ps.product_name AS STRING)        AS product_name,
            CAST(ps.category AS STRING)            AS category,
            CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE)   AS total_revenue,
            CAST(SUM(CAST(sts.quantity AS INT)) AS INT)        AS total_quantity
        FROM sales_transactions_silver sts
        INNER JOIN products_silver ps
            ON sts.product_id = ps.product_id
        GROUP BY
            ps.product_id,
            ps.product_name,
            ps.category
    ),
    thresholds AS (
        SELECT
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_revenue) AS p90_total_revenue
        FROM product_totals
    )
    SELECT
        pt.product_id,
        pt.product_name,
        pt.category,
        pt.total_revenue,
        pt.total_quantity,
        CASE
            WHEN pt.total_revenue >= th.p90_total_revenue THEN TRUE
            ELSE FALSE
        END AS top_product_flag
    FROM product_totals pt
    CROSS JOIN thresholds th
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ======================================================
# Target: gold_data_quality (gdq)
# ======================================================
gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqcs.record_id AS STRING)            AS record_id,
        CAST(dqcs.source_table AS STRING)         AS source_table,
        CAST(dqcs.is_valid AS BOOLEAN)            AS is_valid,
        CAST(dqcs.error_description AS STRING)    AS error_description,
        CAST(dqcs.processing_date AS DATE)        AS processing_date
    FROM data_quality_checks_silver dqcs
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

# ======================================================
# Target: gold_metadata_management (gmm)
# ======================================================
gold_metadata_management_df = spark.sql(
    """
    SELECT
        CAST(mts.data_source AS STRING)               AS data_source,
        CAST(mts.data_ingestion_date AS DATE)         AS data_ingestion_date,
        CAST(mts.data_processing_date AS TIMESTAMP)   AS data_processing_date,
        CAST(mts.data_lineage AS STRING)              AS data_lineage,
        CAST(mts.compliance_status AS STRING)         AS compliance_status
    FROM metadata_tracking_silver mts
    """
)

(
    gold_metadata_management_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_metadata_management.csv")
)

job.commit()
