import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ----------------------------
# Read Source Tables from S3
# ----------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
ssss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_sales_summary_silver.{FILE_FORMAT}/")
)
psss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_sales_summary_silver.{FILE_FORMAT}/")
)
dvs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_validation_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
sts_df.createOrReplaceTempView("sts")
ssss_df.createOrReplaceTempView("ssss")
psss_df.createOrReplaceTempView("psss")
dvs_df.createOrReplaceTempView("dvs")

# ============================================================
# Target: gold_sales_performance
# Source: silver.sales_transactions_silver sts
# ============================================================
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.transaction_date) AS transaction_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue
    FROM sts
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ============================================================
# Target: gold_store_performance
# Source: silver.store_sales_summary_silver ssss
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ssss.store_id AS STRING) AS store_id,
        CAST(ssss.store_name AS STRING) AS store_name,
        CAST(ssss.region AS STRING) AS region,
        CAST(ssss.total_revenue AS DOUBLE) AS total_revenue,
        CAST(ssss.total_transactions AS BIGINT) AS total_transactions,
        CAST(ssss.total_quantity_sold AS BIGINT) AS total_quantity_sold
    FROM ssss
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold_product_performance
# Source: silver.product_sales_summary_silver psss
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(psss.product_id AS STRING) AS product_id,
        CAST(psss.product_name AS STRING) AS product_name,
        CAST(psss.category AS STRING) AS category,
        CAST(psss.total_revenue AS DOUBLE) AS total_revenue,
        CAST(psss.total_quantity_sold AS BIGINT) AS total_quantity_sold
    FROM psss
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_cleaned_data
# Source: silver.data_validation_silver dvs
# ============================================================
gold_cleaned_data_df = spark.sql(
    """
    SELECT
        CAST(dvs.validated_transaction_id AS STRING) AS validated_transaction_id,
        CAST(dvs.validated_store_id AS STRING) AS validated_store_id,
        CAST(dvs.validated_product_id AS STRING) AS validated_product_id
    FROM dvs
    """
)

(
    gold_cleaned_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_data.csv")
)

job.commit()