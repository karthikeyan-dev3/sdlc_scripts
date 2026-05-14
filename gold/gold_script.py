
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

# =========================
# 1) Read source tables
# =========================
product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

daily_sales_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregates_silver.{FILE_FORMAT}/")
)

# =========================
# 2) Create temp views
# =========================
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
daily_sales_aggregates_silver_df.createOrReplaceTempView("daily_sales_aggregates_silver")

# ==========================================================
# TARGET: gold_product_master
# ==========================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)   AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING)     AS category,
        CAST(pms.price AS FLOAT)         AS price
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

# ==========================================================
# TARGET: gold_store_master
# ==========================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)         AS store_id,
        CAST(sms.store_name AS STRING)       AS store_name,
        CAST(sms.store_location AS STRING)   AS store_location,
        CAST(sms.region AS STRING)           AS region
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

# ==========================================================
# TARGET: gold_sales_transactions
# ==========================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)        AS transaction_id,
        DATE(sts.transaction_date)                AS transaction_date,
        CAST(sts.store_id AS STRING)              AS store_id,
        CAST(sts.product_id AS STRING)            AS product_id,
        CAST(sts.quantity_sold AS INT)            AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE)         AS total_revenue
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ==========================================================
# TARGET: gold_sales_aggregates
# ==========================================================
gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        CAST(dsas.store_id AS STRING)             AS store_id,
        CAST(dsas.product_id AS STRING)           AS product_id,
        DATE(dsas.date)                           AS date,
        CAST(dsas.total_quantity_sold AS INT)     AS total_quantity_sold,
        CAST(dsas.total_revenue AS DOUBLE)        AS total_revenue,
        CAST(dsas.average_price AS DOUBLE)        AS average_price
    FROM daily_sales_aggregates_silver dsas
    INNER JOIN product_master_silver pms
        ON dsas.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON dsas.store_id = sms.store_id
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