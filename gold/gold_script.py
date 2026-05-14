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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregates_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ps_df.createOrReplaceTempView("products_silver")
ss_df.createOrReplaceTempView("stores_silver")
dsas_df.createOrReplaceTempView("daily_sales_aggregates_silver")

# ------------------------------------------------------------------------------
# Target: gold_sales_transactions
# ------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.revenue AS DOUBLE) AS revenue
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

# ------------------------------------------------------------------------------
# Target: gold_products_enriched
# ------------------------------------------------------------------------------
gold_products_enriched_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.pricing AS FLOAT) AS pricing
    FROM products_silver ps
    """
)

(
    gold_products_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products_enriched.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_stores_enriched
# ------------------------------------------------------------------------------
gold_stores_enriched_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location
    FROM stores_silver ss
    """
)

(
    gold_stores_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores_enriched.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_sales_aggregated
# ------------------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(dsas.date) AS date,
        CAST(dsas.product_id AS STRING) AS product_id,
        CAST(dsas.store_id AS STRING) AS store_id,
        CAST(dsas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(dsas.total_revenue AS DOUBLE) AS total_revenue
    FROM daily_sales_aggregates_silver dsas
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