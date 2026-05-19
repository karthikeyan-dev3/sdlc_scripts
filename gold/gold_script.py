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

# ----------------------------
# Read Source Tables from S3
# ----------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

daily_sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregated_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
daily_sales_aggregated_silver_df.createOrReplaceTempView("daily_sales_aggregated_silver")

# ============================================================
# Target Table: gold_products
# Source: products_silver ps
# ============================================================
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category
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

# ============================================================
# Target Table: gold_stores
# Source: stores_silver ss
# ============================================================
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)   AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING)   AS location
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

# ============================================================
# Target Table: gold_sales
# Source: sales_transactions_silver sts
# ============================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.quantity AS INT)          AS quantity,
        CAST(sts.revenue AS DOUBLE)        AS revenue,
        DATE(sts.transaction_date)         AS transaction_date
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

# ============================================================
# Target Table: gold_sales_aggregated
# Source: daily_sales_aggregated_silver dsas
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(dsas.store_id AS STRING)      AS store_id,
        CAST(dsas.product_id AS STRING)    AS product_id,
        CAST(dsas.total_quantity AS INT)   AS total_quantity,
        CAST(dsas.total_revenue AS DOUBLE) AS total_revenue,
        CAST(dsas.average_price AS DOUBLE) AS average_price,
        DATE(dsas.sales_date)              AS sales_date
    FROM daily_sales_aggregated_silver dsas
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