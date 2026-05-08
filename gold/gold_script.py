from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import sys

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------
# Read Source Tables (S3)
# -------------------------
product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create Temp Views
# -------------------------
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ============================================================
# Target: gold.gold_product_master
# Source: silver.product_master_silver pms
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)      AS product_id,
        CAST(pms.product_name AS STRING)    AS product_name,
        CAST(pms.category AS STRING)        AS category,
        CAST(pms.brand AS STRING)           AS brand,
        CAST(pms.price AS DECIMAL(38, 18))  AS price
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
# Target: gold.gold_store_master
# Source: silver.store_master_silver sms
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)     AS store_id,
        CAST(sms.store_name AS STRING)   AS store_name,
        CAST(sms.location AS STRING)     AS location,
        CAST(sms.store_type AS STRING)   AS store_type
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
# Target: gold.gold_sales_transactions
# Source: silver.sales_transactions_silver sts
#         LEFT JOIN silver.product_master_silver pms ON sts.product_id = pms.product_id
#         LEFT JOIN silver.store_master_silver sms   ON sts.store_id = sms.store_id
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)           AS transaction_id,
        CAST(sts.transaction_date AS DATE)           AS transaction_date,
        CAST(sts.product_id AS STRING)               AS product_id,
        CAST(sts.store_id AS STRING)                 AS store_id,
        CAST(sts.quantity_sold AS INT)               AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)       AS total_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
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

# ============================================================
# Target: gold.gold_aggregated_sales
# Source: silver.aggregated_sales_silver ass
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING)             AS store_id,
        CAST(ass.product_id AS STRING)           AS product_id,
        CAST(ass.transaction_date AS DATE)       AS transaction_date,
        CAST(ass.total_quantity_sold AS INT)     AS total_quantity_sold,
        CAST(ass.total_sales_amount AS DOUBLE)   AS total_sales_amount,
        CAST(ass.average_price AS DOUBLE)        AS average_price
    FROM aggregated_sales_silver ass
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()
