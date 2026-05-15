import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Read Source Tables (S3)
# -----------------------------
product_information_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_information_silver.{FILE_FORMAT}/")
)

store_information_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")
)

sales_transaction_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transaction_details_silver.{FILE_FORMAT}/")
)

sales_summary_reports_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_summary_reports_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
product_information_silver_df.createOrReplaceTempView("product_information_silver")
store_information_silver_df.createOrReplaceTempView("store_information_silver")
sales_transaction_details_silver_df.createOrReplaceTempView("sales_transaction_details_silver")
sales_summary_reports_silver_df.createOrReplaceTempView("sales_summary_reports_silver")

# -----------------------------
# Target: gold_product_master
# Source: silver.product_information_silver pis
# -----------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pis.product_id AS STRING)        AS product_id,
        CAST(pis.product_name AS STRING)      AS product_name,
        CAST(pis.product_category AS STRING)  AS product_category,
        CAST(pis.price AS FLOAT)              AS price
    FROM product_information_silver pis
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------
# Target: gold_store_master
# Source: silver.store_information_silver sis
# -----------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sis.store_id AS STRING)         AS store_id,
        CAST(sis.store_name AS STRING)       AS store_name,
        CAST(sis.store_location AS STRING)   AS store_location,
        CAST(sis.store_region AS STRING)     AS store_region
    FROM store_information_silver sis
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------
# Target: gold_sales_performance
# Source: silver.sales_transaction_details_silver stds
# -----------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(stds.transaction_id AS STRING)      AS transaction_id,
        DATE(stds.sales_date)                    AS sales_date,
        CAST(stds.product_id AS STRING)          AS product_id,
        CAST(stds.store_id AS STRING)            AS store_id,
        CAST(stds.quantity_sold AS INT)          AS quantity_sold,
        CAST(stds.sales_amount AS DOUBLE)        AS sales_amount,
        CAST(stds.product_category AS STRING)    AS product_category,
        CAST(stds.store_region AS STRING)        AS store_region
    FROM sales_transaction_details_silver stds
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# -----------------------------
# Target: gold_sales_aggregates
# Source: silver.sales_summary_reports_silver ssrs
# -----------------------------
gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        DATE(ssrs.reporting_date)                AS reporting_date,
        CAST(ssrs.store_id AS STRING)            AS store_id,
        CAST(ssrs.product_category AS STRING)    AS product_category,
        CAST(ssrs.total_quantity_sold AS INT)    AS total_quantity_sold,
        CAST(ssrs.total_sales_amount AS DOUBLE)  AS total_sales_amount,
        CAST(ssrs.average_sales_price AS DOUBLE) AS average_sales_price
    FROM sales_summary_reports_silver ssrs
    """
)

(
    gold_sales_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)

job.commit()