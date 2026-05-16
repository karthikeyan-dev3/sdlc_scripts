import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# ============================================================
# Read source tables from S3 (Silver)
# ============================================================

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

aggregated_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_daily_silver.{FILE_FORMAT}/")
)
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

# ============================================================
# Target: gold.sales_transactions_gold
# Mapping: silver.sales_transactions_silver sts
# ============================================================

sales_transactions_gold_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)              AS transaction_id,
        CAST(sts.transaction_date AS DATE)             AS transaction_date,
        CAST(sts.product_id AS STRING)                 AS product_id,
        CAST(sts.store_id AS STRING)                   AS store_id,
        CAST(sts.quantity_sold AS INT)                 AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)         AS total_sales_amount
    FROM sales_transactions_silver sts
    """
)

sales_transactions_gold_df = sales_transactions_gold_df.coalesce(1)

(
    sales_transactions_gold_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_transactions_gold.csv")
)

# ============================================================
# Target: gold.product_master_gold
# Mapping: silver.product_master_silver pms
# ============================================================

product_master_gold_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)     AS product_id,
        CAST(pms.product_name AS STRING)   AS product_name,
        CAST(pms.category AS STRING)       AS category,
        CAST(pms.brand AS STRING)          AS brand
    FROM product_master_silver pms
    """
)

product_master_gold_df = product_master_gold_df.coalesce(1)

(
    product_master_gold_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/product_master_gold.csv")
)

# ============================================================
# Target: gold.store_master_gold
# Mapping: silver.store_master_silver sms
# ============================================================

store_master_gold_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)       AS store_id,
        CAST(sms.store_name AS STRING)     AS store_name,
        CAST(sms.location AS STRING)       AS location,
        CAST(sms.store_type AS STRING)     AS store_type
    FROM store_master_silver sms
    """
)

store_master_gold_df = store_master_gold_df.coalesce(1)

(
    store_master_gold_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/store_master_gold.csv")
)

# ============================================================
# Target: gold.aggregated_sales_gold
# Mapping: silver.aggregated_sales_daily_silver asds
# ============================================================

aggregated_sales_gold_df = spark.sql(
    """
    SELECT
        CAST(asds.date AS DATE)                        AS date,
        CAST(asds.store_id AS STRING)                  AS store_id,
        CAST(asds.product_id AS STRING)                AS product_id,
        CAST(asds.total_sales_amount AS DOUBLE)        AS total_sales_amount,
        CAST(asds.total_quantity_sold AS INT)          AS total_quantity_sold,
        CAST(asds.number_of_transactions AS BIGINT)    AS number_of_transactions
    FROM aggregated_sales_daily_silver asds
    """
)

aggregated_sales_gold_df = aggregated_sales_gold_df.coalesce(1)

(
    aggregated_sales_gold_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/aggregated_sales_gold.csv")
)

job.commit()