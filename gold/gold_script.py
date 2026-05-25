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

# -----------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -----------------------------------------------------------------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("products_silver")

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("stores_silver")

ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
ass_df.createOrReplaceTempView("aggregated_sales_silver")

# -----------------------------------------------------------------------------------
# Target: gold.gold_sales_transactions
# Mapping: silver.sales_transactions_silver sts
# -----------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)                 AS transaction_id,
        CAST(sts.product_id AS STRING)                     AS product_id,
        CAST(sts.store_id AS STRING)                       AS store_id,
        CAST(sts.transaction_date AS DATE)                 AS transaction_date,
        CAST(sts.sales_amount AS DOUBLE)                   AS sales_amount,
        CAST(sts.quantity_sold AS INT)                     AS quantity_sold,
        CAST(sts.cleaned_data_flag AS BOOLEAN)             AS cleaned_data_flag
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

# -----------------------------------------------------------------------------------
# Target: gold.gold_product_master
# Mapping: silver.products_silver ps
# -----------------------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)          AS product_id,
        CAST(ps.product_name AS STRING)        AS product_name,
        CAST(ps.category AS STRING)            AS category,
        CAST(ps.price AS FLOAT)                AS price
    FROM products_silver ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_store_master
# Mapping: silver.stores_silver ss
# -----------------------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)            AS store_id,
        CAST(ss.store_name AS STRING)          AS store_name,
        CAST(ss.region AS STRING)              AS region,
        CAST(ss.store_type AS STRING)          AS store_type
    FROM stores_silver ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_aggregated_sales
# Mapping: silver.aggregated_sales_silver ass
# -----------------------------------------------------------------------------------

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING)               AS store_id,
        CAST(ass.product_id AS STRING)             AS product_id,
        CAST(ass.total_sales_amount AS DOUBLE)     AS total_sales_amount,
        CAST(ass.total_quantity_sold AS INT)       AS total_quantity_sold,
        CAST(ass.reporting_period AS STRING)       AS reporting_period
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