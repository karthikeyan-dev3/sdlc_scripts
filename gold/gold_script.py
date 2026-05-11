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
# Read Source Tables (S3) + Create Temp Views
# -------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

store_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_sales_daily_silver.{FILE_FORMAT}/")
)
store_sales_daily_silver_df.createOrReplaceTempView("store_sales_daily_silver")

product_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_sales_daily_silver.{FILE_FORMAT}/")
)
product_sales_daily_silver_df.createOrReplaceTempView("product_sales_daily_silver")

sales_daily_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_daily_summary_silver.{FILE_FORMAT}/")
)
sales_daily_summary_silver_df.createOrReplaceTempView("sales_daily_summary_silver")

data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# -------------------------------------------------------------------
# Target: gold_sales
# Sources: sales_transactions_silver sts
#          LEFT JOIN products_silver ps ON sts.product_id = ps.product_id
#          LEFT JOIN stores_silver ss ON sts.store_id = ss.store_id
# -------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)          AS transaction_id,
        CAST(sts.product_id AS STRING)              AS product_id,
        CAST(ps.product_name AS STRING)             AS product_name,
        CAST(ps.product_category AS STRING)         AS product_category,
        CAST(sts.store_id AS STRING)                AS store_id,
        CAST(ss.store_name AS STRING)               AS store_name,
        CAST(ss.store_location AS STRING)           AS store_location,
        DATE(sts.sale_date)                         AS sale_date,
        CAST(sts.quantity_sold AS INT)              AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)      AS total_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

gold_sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_sales.csv")

# -------------------------------------------------------------------
# Target: gold_store_performance
# Sources: store_sales_daily_silver ssds
# -------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ssds.store_id AS STRING)               AS store_id,
        CAST(ssds.store_name AS STRING)             AS store_name,
        CAST(ssds.store_location AS STRING)         AS store_location,
        DATE(ssds.date)                              AS date,
        CAST(ssds.total_sales AS DOUBLE)            AS total_sales,
        CAST(ssds.number_of_transactions AS BIGINT) AS number_of_transactions
    FROM store_sales_daily_silver ssds
    """
)

gold_store_performance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/gold_store_performance.csv"
)

# -------------------------------------------------------------------
# Target: gold_product_performance
# Sources: product_sales_daily_silver psds
# -------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(psds.product_id AS STRING)     AS product_id,
        CAST(psds.product_name AS STRING)   AS product_name,
        CAST(psds.product_category AS STRING) AS product_category,
        DATE(psds.date)                     AS date,
        CAST(psds.total_sales AS DOUBLE)    AS total_sales,
        CAST(psds.quantity_sold AS BIGINT)  AS quantity_sold
    FROM product_sales_daily_silver psds
    """
)

gold_product_performance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/gold_product_performance.csv"
)

# -------------------------------------------------------------------
# Target: gold_aggregated_sales
# Sources: sales_daily_summary_silver sdss
# -------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(sdss.date)                          AS date,
        CAST(sdss.total_sales AS DOUBLE)         AS total_sales,
        CAST(sdss.total_quantity_sold AS BIGINT) AS total_quantity_sold,
        CAST(sdss.number_of_transactions AS BIGINT) AS number_of_transactions
    FROM sales_daily_summary_silver sdss
    """
)

gold_aggregated_sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/gold_aggregated_sales.csv"
)

# -------------------------------------------------------------------
# Target: gold_data_quality_reports
# Sources: data_quality_metrics_silver dqms
# -------------------------------------------------------------------
gold_data_quality_reports_df = spark.sql(
    """
    SELECT
        DATE(dqms.date)                               AS date,
        CAST(dqms.total_records_processed AS BIGINT)  AS total_records_processed,
        CAST(dqms.duplicates_removed AS BIGINT)       AS duplicates_removed
    FROM data_quality_metrics_silver dqms
    """
)

gold_data_quality_reports_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/gold_data_quality_reports.csv"
)

job.commit()
