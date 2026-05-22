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

# ------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------------------------

sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------------------------
# Target: gold_sales_data
# Source: silver.sales_enriched_silver ses
# ------------------------------------------------------------------------------

gold_sales_data_df = spark.sql(
    """
    SELECT
        CAST(ses.transaction_id AS STRING)     AS transaction_id,
        CAST(ses.transaction_date AS DATE)     AS transaction_date,
        CAST(ses.product_id AS STRING)         AS product_id,
        CAST(ses.product_name AS STRING)       AS product_name,
        CAST(ses.store_id AS STRING)           AS store_id,
        CAST(ses.store_name AS STRING)         AS store_name,
        CAST(ses.quantity_sold AS INT)         AS quantity_sold,
        CAST(ses.sales_amount AS DOUBLE)       AS sales_amount,
        CAST(ses.category AS STRING)           AS category,
        CAST(ses.region AS STRING)             AS region
    FROM sales_enriched_silver ses
    """
)

(
    gold_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_data.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_store_performance
# Source: silver.aggregated_sales_silver ass INNER JOIN silver.stores_silver ss
# ------------------------------------------------------------------------------

gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING)                   AS store_id,
        CAST(ss.store_name AS STRING)                  AS store_name,
        CAST(ass.total_sales AS DOUBLE)                AS total_sales,
        CAST(ass.total_units_sold AS INT)              AS total_units_sold,
        CAST(ass.average_transaction_value AS DOUBLE)  AS average_transaction_value,
        CAST(ass.report_date AS DATE)                  AS report_date
    FROM aggregated_sales_silver ass
    INNER JOIN stores_silver ss
        ON ass.store_id = ss.store_id
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_product_performance
# Source: silver.aggregated_sales_silver ass INNER JOIN silver.products_silver ps
# ------------------------------------------------------------------------------

gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ass.product_id AS STRING)      AS product_id,
        CAST(ps.product_name AS STRING)     AS product_name,
        CAST(ass.total_sales AS DOUBLE)     AS total_sales,
        CAST(ass.total_units_sold AS INT)   AS total_units_sold,
        CAST(ass.average_price AS DOUBLE)   AS average_price,
        CAST(ass.report_date AS DATE)       AS report_date
    FROM aggregated_sales_silver ass
    INNER JOIN products_silver ps
        ON ass.product_id = ps.product_id
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()