import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read source tables from S3 (CSV) and create temp views
# ------------------------------------------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

pds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)
pds_df.createOrReplaceTempView("product_details_silver")

sis_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")
)
sis_df.createOrReplaceTempView("store_information_silver")

# ------------------------------------------------------------
# Target: gold.gold_sales
# Source: silver.sales_transactions_silver sts
# Columns: transaction_id, product_id, store_id, date, quantity_sold, total_revenue
# ------------------------------------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        sts.date AS date,
        sts.quantity_sold AS quantity_sold,
        sts.total_revenue AS total_revenue
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

# ------------------------------------------------------------
# Target: gold.gold_product_master
# Source: silver.product_details_silver pds
# Columns: product_id, product_name, category, price
# ------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        pds.product_id AS product_id,
        pds.product_name AS product_name,
        pds.category AS category,
        pds.price AS price
    FROM product_details_silver pds
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------
# Target: gold.gold_store_master
# Source: silver.store_information_silver sis
# Columns: store_id, store_name, location
# ------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        sis.store_id AS store_id,
        sis.store_name AS store_name,
        sis.location AS location
    FROM store_information_silver sis
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

job.commit()
