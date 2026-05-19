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

# --------------------------------------------------------------------
# 1) Read source tables
# --------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
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

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")

# --------------------------------------------------------------------
# TARGET: gold_sales_transactions
# --------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)         AS transaction_id,
        CAST(sts.product_id AS STRING)             AS product_id,
        CAST(sts.store_id AS STRING)               AS store_id,
        CAST(sts.transaction_date AS DATE)         AS transaction_date,
        CAST(sts.quantity_sold AS INT)             AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)     AS total_sales_amount
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

# --------------------------------------------------------------------
# TARGET: gold_product_master
# --------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)     AS product_id,
        CAST(pms.product_name AS STRING)   AS product_name,
        CAST(pms.category AS STRING)       AS category,
        CAST(pms.price AS FLOAT)           AS price,
        CAST(pms.brand AS STRING)          AS brand
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

# --------------------------------------------------------------------
# TARGET: gold_store_master
# --------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)       AS store_id,
        CAST(sms.store_name AS STRING)     AS store_name,
        CAST(sms.location AS STRING)       AS location,
        CAST(sms.region AS STRING)         AS region,
        CAST(sms.store_type AS STRING)     AS store_type
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

# --------------------------------------------------------------------
# TARGET: gold_sales_aggregated
# --------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_date AS DATE)         AS date,
        CAST(sms.region AS STRING)                 AS region,
        CAST(pms.category AS STRING)               AS product_category,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)          AS total_quantity_sold,
        CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    GROUP BY
        CAST(sts.transaction_date AS DATE),
        CAST(sms.region AS STRING),
        CAST(pms.category AS STRING)
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()