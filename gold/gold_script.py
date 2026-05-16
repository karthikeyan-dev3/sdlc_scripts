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

# ----------------------------
# Read Source Tables (Silver)
# ----------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

# ----------------------------
# Target: gold_products
# ----------------------------
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS price
    FROM products_silver ps
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# ----------------------------
# Target: gold_stores
# ----------------------------
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)        AS store_id,
        CAST(ss.store_name AS STRING)      AS store_name,
        CAST(ss.region AS STRING)          AS region,
        CAST(ss.store_manager AS STRING)   AS store_manager
    FROM stores_silver ss
    """
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

# ----------------------------
# Target: gold_sales_transactions
# ----------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)   AS transaction_id,
        DATE(CAST(sts.date AS STRING))       AS date,
        CAST(sts.product_id AS STRING)       AS product_id,
        CAST(sts.store_id AS STRING)         AS store_id,
        CAST(sts.quantity_sold AS INT)       AS quantity_sold,
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

# ----------------------------
# Target: gold_sales_aggregated
# ----------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(CAST(sas.report_date AS STRING))   AS report_date,
        CAST(sas.product_id AS STRING)          AS product_id,
        CAST(sas.store_id AS STRING)            AS store_id,
        CAST(sas.total_quantity_sold AS INT)    AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE)  AS total_sales_amount,
        CAST(sas.average_price AS DOUBLE)       AS average_price
    FROM sales_aggregated_silver sas
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