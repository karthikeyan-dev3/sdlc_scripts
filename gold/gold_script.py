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

# --------------------------------------------------------------------------------------
# Read Source Tables from S3
# --------------------------------------------------------------------------------------
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

dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregate_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# Create Temp Views
# --------------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
dsas_df.createOrReplaceTempView("daily_sales_aggregate_silver")

# --------------------------------------------------------------------------------------
# Target: gold_sales_transactions
# --------------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        DATE(sts.transaction_date) AS transaction_date,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
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

# --------------------------------------------------------------------------------------
# Target: gold_product_master
# --------------------------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand,
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

# --------------------------------------------------------------------------------------
# Target: gold_store_master
# --------------------------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.region AS STRING) AS region,
        CAST(sms.store_type AS STRING) AS store_type
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

# --------------------------------------------------------------------------------------
# Target: gold_aggregated_sales
# --------------------------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(dsas.reporting_date) AS reporting_date,
        CAST(dsas.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(dsas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(dsas.average_sales_per_transaction AS DOUBLE) AS average_sales_per_transaction,
        CAST(dsas.sales_by_category AS STRING) AS sales_by_category,
        CAST(dsas.sales_by_region AS STRING) AS sales_by_region
    FROM daily_sales_aggregate_silver dsas
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