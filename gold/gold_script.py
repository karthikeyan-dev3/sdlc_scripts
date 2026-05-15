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

# -----------------------------
# Read Source Tables (S3 -> DF)
# -----------------------------
pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

sdss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_store_silver.{FILE_FORMAT}/")
)

sdps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_product_silver.{FILE_FORMAT}/")
)

sdrs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_refresh_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
sts_df.createOrReplaceTempView("sales_transactions_silver")
sdss_df.createOrReplaceTempView("sales_daily_store_silver")
sdps_df.createOrReplaceTempView("sales_daily_product_silver")
sdrs_df.createOrReplaceTempView("sales_daily_refresh_silver")

# ============================================================
# Target: gold_product_master (gold.gold_product_master)
# Source: silver.product_master_silver pms
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)        AS product_id,
        CAST(pms.product_name AS STRING)      AS product_name,
        CAST(pms.category AS STRING)          AS category,
        CAST(pms.brand AS STRING)             AS brand,
        CAST(pms.price AS FLOAT)              AS price
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ============================================================
# Target: gold_store_master (gold.gold_store_master)
# Source: silver.store_master_silver sms
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)      AS store_id,
        CAST(sms.store_name AS STRING)    AS store_name,
        CAST(sms.location AS STRING)      AS location,
        CAST(sms.region AS STRING)        AS region
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ============================================================
# Target: gold_sales_transactions (gold.gold_sales_transactions)
# Source: silver.sales_transactions_silver sts
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)        AS transaction_id,
        CAST(sts.store_id AS STRING)              AS store_id,
        CAST(sts.product_id AS STRING)            AS product_id,
        DATE(sts.transaction_date)                AS transaction_date,
        CAST(sts.quantity_sold AS INT)            AS quantity_sold,
        CAST(sts.total_sales AS DOUBLE)           AS total_sales
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ============================================================
# Target: gold_sales_aggregated_store (gold.gold_sales_aggregated_store)
# Source: silver.sales_daily_store_silver sdss
# ============================================================
gold_sales_aggregated_store_df = spark.sql(
    """
    SELECT
        CAST(sdss.store_id AS STRING)             AS store_id,
        DATE(sdss.date)                           AS date,
        CAST(sdss.total_revenue AS DOUBLE)        AS total_revenue,
        CAST(sdss.total_quantity_sold AS INT)     AS total_quantity_sold
    FROM sales_daily_store_silver sdss
    """
)

(
    gold_sales_aggregated_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_store.csv")
)

# ============================================================
# Target: gold_sales_aggregated_product (gold.gold_sales_aggregated_product)
# Source: silver.sales_daily_product_silver sdps
# ============================================================
gold_sales_aggregated_product_df = spark.sql(
    """
    SELECT
        CAST(sdps.product_id AS STRING)           AS product_id,
        DATE(sdps.date)                           AS date,
        CAST(sdps.total_revenue AS DOUBLE)        AS total_revenue,
        CAST(sdps.total_quantity_sold AS INT)     AS total_quantity_sold
    FROM sales_daily_product_silver sdps
    """
)

(
    gold_sales_aggregated_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_product.csv")
)

# ============================================================
# Target: gold_sales_daily_refresh (gold.gold_sales_daily_refresh)
# Source: silver.sales_daily_refresh_silver sdrs
# ============================================================
gold_sales_daily_refresh_df = spark.sql(
    """
    SELECT
        DATE(sdrs.refresh_date)                   AS refresh_date,
        CAST(sdrs.records_processed AS INT)       AS records_processed
    FROM sales_daily_refresh_silver sdrs
    """
)

(
    gold_sales_daily_refresh_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_refresh.csv")
)

job.commit()