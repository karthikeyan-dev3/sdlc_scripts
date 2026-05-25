import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -------------------------------------------------------------------

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

dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_agg_silver.{FILE_FORMAT}/")
)
dsas_df.createOrReplaceTempView("daily_sales_agg_silver")

# -------------------------------------------------------------------
# Target: gold_sales
# -------------------------------------------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)       AS transaction_id,
        DATE(sts.transaction_date)              AS transaction_date,
        CAST(sts.store_id AS STRING)            AS store_id,
        CAST(sts.product_id AS STRING)          AS product_id,
        CAST(sts.sales_amount AS DOUBLE)        AS sales_amount,
        CAST(sts.quantity_sold AS INT)          AS quantity_sold
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

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)          AS product_id,
        CAST(pms.product_name AS STRING)        AS product_name,
        CAST(pms.category AS STRING)            AS category,
        CAST(pms.brand AS STRING)               AS brand
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

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)            AS store_id,
        CAST(sms.store_name AS STRING)          AS store_name,
        CAST(sms.region AS STRING)              AS region,
        CAST(sms.store_type AS STRING)          AS store_type
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

# -------------------------------------------------------------------
# Target: gold_aggregated_sales
# -------------------------------------------------------------------

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(dsas.store_id AS STRING)           AS store_id,
        CAST(dsas.product_id AS STRING)         AS product_id,
        DATE(dsas.date)                         AS date,
        CAST(dsas.total_sales AS DOUBLE)        AS total_sales,
        CAST(dsas.total_quantity AS INT)        AS total_quantity,
        CAST(dsas.average_sales_price AS DOUBLE) AS average_sales_price
    FROM daily_sales_agg_silver dsas
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
