import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables from S3
# -----------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("product_master_silver")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("store_master_silver")

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_silver.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("sales_performance_silver")

# -----------------------------
# gold_sales_transactions
# -----------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.revenue AS DOUBLE) AS revenue,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
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

# -----------------------------
# gold_product_master
# -----------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.price AS FLOAT) AS price
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

# -----------------------------
# gold_store_master
# -----------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.region AS STRING) AS region,
        CAST(sms.address AS STRING) AS address
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

# -----------------------------
# gold_sales_performance
# -----------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.store_id AS STRING) AS store_id,
        CAST(sps.product_id AS STRING) AS product_id,
        CAST(sps.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sps.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sps.average_price AS DOUBLE) AS average_price
    FROM sales_performance_silver sps
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