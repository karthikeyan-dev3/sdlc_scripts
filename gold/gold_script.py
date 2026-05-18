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

# ------------------------------------------------------------------------------
# Read Source Tables (Silver)
# ------------------------------------------------------------------------------

store_daily_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_daily_sales_silver.{FILE_FORMAT}/")
)

product_daily_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_daily_sales_silver.{FILE_FORMAT}/")
)

product_daily_rank_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_daily_rank_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------------------

store_daily_sales_silver_df.createOrReplaceTempView("store_daily_sales_silver")
product_daily_sales_silver_df.createOrReplaceTempView("product_daily_sales_silver")
product_daily_rank_silver_df.createOrReplaceTempView("product_daily_rank_silver")

# ------------------------------------------------------------------------------
# Target: gold_store_performance
# ------------------------------------------------------------------------------

gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sdss.store_id AS STRING)              AS store_id,
        CAST(sdss.store_name AS STRING)            AS store_name,
        CAST(sdss.city AS STRING)                  AS city,
        CAST(sdss.store_type AS STRING)            AS store_type,
        CAST(sdss.total_revenue AS DOUBLE)         AS total_revenue,
        CAST(sdss.transaction_count AS INT)        AS transaction_count,
        CAST(sdss.quantity_sold AS INT)            AS quantity_sold,
        DATE(CAST(sdss.date AS STRING))            AS date
    FROM store_daily_sales_silver sdss
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_product_performance
# ------------------------------------------------------------------------------

gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(pdss.product_id AS STRING)            AS product_id,
        CAST(pdss.product_name AS STRING)          AS product_name,
        CAST(pdss.product_category AS STRING)      AS product_category,
        CAST(pdss.total_revenue AS DOUBLE)         AS total_revenue,
        CAST(pdss.quantity_sold AS INT)            AS quantity_sold,
        CAST(pdrs.top_performer_rank AS INT)       AS top_performer_rank,
        DATE(CAST(pdss.date AS STRING))            AS date
    FROM product_daily_sales_silver pdss
    INNER JOIN product_daily_rank_silver pdrs
        ON pdss.product_id = pdrs.product_id
       AND pdss.date = pdrs.date
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()