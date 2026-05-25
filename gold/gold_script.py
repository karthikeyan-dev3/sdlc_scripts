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

# ------------------------------------------------------------------------------
# Read Source Tables from S3 (Silver)
# ------------------------------------------------------------------------------

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

sales_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")
)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

data_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

# ------------------------------------------------------------------------------
# Target: gold.gold_sales_transactions (gst) from silver.sales_transactions_silver (sts)
# ------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue
    FROM sales_transactions_silver sts
    """
)

gold_sales_transactions_df.coalesce(1).write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(
    f"{TARGET_PATH}/gold_sales_transactions.csv"
)

# ------------------------------------------------------------------------------
# Target: gold.gold_product_master (gpm) from silver.product_master_silver (pms)
# ------------------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand
    FROM product_master_silver pms
    """
)

gold_product_master_df.coalesce(1).write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(
    f"{TARGET_PATH}/gold_product_master.csv"
)

# ------------------------------------------------------------------------------
# Target: gold.gold_store_master (gsm) from silver.store_master_silver (sms)
# ------------------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.region AS STRING) AS region
    FROM store_master_silver sms
    """
)

gold_store_master_df.coalesce(1).write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(
    f"{TARGET_PATH}/gold_store_master.csv"
)

# ------------------------------------------------------------------------------
# Target: gold.gold_sales_aggregates (gsa) from silver.sales_aggregates_silver (sas)
# ------------------------------------------------------------------------------

gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.product_id AS STRING) AS product_id,
        CAST(sas.date AS DATE) AS date,
        CAST(sas.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sas.transaction_count AS BIGINT) AS transaction_count,
        CAST(sas.total_quantity_sold AS BIGINT) AS total_quantity_sold
    FROM sales_aggregates_silver sas
    """
)

gold_sales_aggregates_df.coalesce(1).write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(
    f"{TARGET_PATH}/gold_sales_aggregates.csv"
)

# ------------------------------------------------------------------------------
# Target: gold.gold_data_quality (gdq) from silver.data_quality_silver (dqs)
# ------------------------------------------------------------------------------

gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqs.record_id AS STRING) AS record_id,
        CAST(dqs.source_system AS STRING) AS source_system,
        CAST(dqs.validation_status AS STRING) AS validation_status,
        CAST(dqs.issues_found AS STRING) AS issues_found
    FROM data_quality_silver dqs
    """
)

gold_data_quality_df.coalesce(1).write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(
    f"{TARGET_PATH}/gold_data_quality.csv"
)

job.commit()