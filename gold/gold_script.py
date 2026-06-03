import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# -----------------------------
# Read sources + Temp Views
# -----------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("products_silver")

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("stores_silver")

sdps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_daily_performance_silver.{FILE_FORMAT}/")
)
sdps_df.createOrReplaceTempView("store_daily_performance_silver")

pdps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_daily_performance_silver.{FILE_FORMAT}/")
)
pdps_df.createOrReplaceTempView("product_daily_performance_silver")

ars_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_reports_silver.{FILE_FORMAT}/")
)
ars_df.createOrReplaceTempView("aggregated_reports_silver")

# -----------------------------
# gold_sales
# -----------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)  AS transaction_id,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(sts.transaction_date AS DATE)  AS transaction_date,
        CAST(sts.revenue AS DOUBLE)         AS revenue,
        CAST(sts.quantity_sold AS INT)      AS quantity_sold
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -----------------------------
# gold_products
# -----------------------------
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)    AS product_id,
        CAST(ps.product_name AS STRING)  AS product_name,
        CAST(ps.category AS STRING)      AS category,
        CAST(ps.price AS FLOAT)          AS price
    FROM products_silver ps
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# -----------------------------
# gold_stores
# -----------------------------
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)     AS store_id,
        CAST(ss.store_name AS STRING)   AS store_name,
        CAST(ss.location AS STRING)     AS location,
        CAST(ss.region AS STRING)       AS region
    FROM stores_silver ss
    """
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

# -----------------------------
# gold_store_performance
# -----------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sdps.store_id AS STRING)          AS store_id,
        CAST(sdps.date AS DATE)               AS date,
        CAST(sdps.total_revenue AS DOUBLE)    AS total_revenue,
        CAST(sdps.transaction_count AS BIGINT) AS transaction_count
    FROM store_daily_performance_silver sdps
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# -----------------------------
# gold_product_performance
# -----------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(pdps.product_id AS STRING)        AS product_id,
        CAST(pdps.date AS DATE)               AS date,
        CAST(pdps.total_revenue AS DOUBLE)    AS total_revenue,
        CAST(pdps.quantity_sold AS BIGINT)    AS quantity_sold
    FROM product_daily_performance_silver pdps
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# -----------------------------
# gold_aggregated_reports
# -----------------------------
gold_aggregated_reports_df = spark.sql(
    """
    SELECT
        CAST(ars.report_id AS STRING)           AS report_id,
        CAST(ars.report_name AS STRING)         AS report_name,
        CAST(ars.generated_on AS TIMESTAMP)     AS generated_on,
        CAST(ars.aggregation_level AS STRING)  AS aggregation_level
    FROM aggregated_reports_silver ars
    """
)

(
    gold_aggregated_reports_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_reports.csv")
)

job.commit()