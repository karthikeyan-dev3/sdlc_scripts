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

# ------------------------------------------------------------------
# Read Source Tables
# ------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

daily_sales_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_summary_silver.{FILE_FORMAT}/")
)

store_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")
)

product_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")
store_details_silver_df.createOrReplaceTempView("store_details_silver")
product_details_silver_df.createOrReplaceTempView("product_details_silver")

# ------------------------------------------------------------------
# Target: cleaned_sales_transactions
# Mapping: silver.sales_transactions_silver sts
# ------------------------------------------------------------------
cleaned_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.sale_date) AS sale_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    """
)

(
    cleaned_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/cleaned_sales_transactions.csv")
)

# ------------------------------------------------------------------
# Target: aggregated_sales_data
# Mapping: silver.daily_sales_summary_silver dss
# ------------------------------------------------------------------
aggregated_sales_data_df = spark.sql(
    """
    SELECT
        DATE(dss.aggregation_date) AS aggregation_date,
        CAST(dss.store_id AS STRING) AS store_id,
        CAST(dss.product_id AS STRING) AS product_id,
        CAST(dss.daily_total_revenue AS DOUBLE) AS daily_total_revenue,
        CAST(dss.daily_transaction_count AS INT) AS daily_transaction_count
    FROM daily_sales_summary_silver dss
    """
)

(
    aggregated_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_data.csv")
)

# ------------------------------------------------------------------
# Target: gold_sales_performance
# Mapping: silver.daily_sales_summary_silver dss
#          LEFT JOIN silver.store_details_silver sds ON dss.store_id = sds.store_id
#          LEFT JOIN silver.product_details_silver pds ON dss.product_id = pds.product_id
# ------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(dss.store_id AS STRING) AS store_id,
        CAST(sds.store_name AS STRING) AS store_name,
        CAST(dss.product_id AS STRING) AS product_id,
        CAST(pds.product_name AS STRING) AS product_name,
        DATE(dss.aggregation_date) AS transaction_date,
        CAST(dss.daily_total_revenue AS DOUBLE) AS total_revenue,
        CAST(dss.daily_total_revenue AS DOUBLE) AS product_revenue,
        CAST(dss.daily_transaction_count AS INT) AS transaction_count
    FROM daily_sales_summary_silver dss
    LEFT JOIN store_details_silver sds
        ON dss.store_id = sds.store_id
    LEFT JOIN product_details_silver pds
        ON dss.product_id = pds.product_id
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

job.commit()
