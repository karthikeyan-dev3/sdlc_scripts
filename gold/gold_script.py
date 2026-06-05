import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ---------------------------------------------------------------------
# Read Source Tables (S3)
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Create Temp Views
# ---------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ---------------------------------------------------------------------
# Target Table: gold_product_master
# ---------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)      AS product_id,
        CAST(ps.product_name AS STRING)    AS product_name,
        CAST(ps.product_category AS STRING) AS product_category,
        CAST(ps.product_brand AS STRING)   AS product_brand,
        CAST(ps.product_price AS DOUBLE)   AS product_price
    FROM products_silver ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_master.csv")
)

# ---------------------------------------------------------------------
# Target Table: gold_store_master
# ---------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)       AS store_id,
        CAST(ss.store_name AS STRING)     AS store_name,
        CAST(ss.store_region AS STRING)   AS store_region,
        CAST(ss.store_manager AS STRING)  AS store_manager
    FROM stores_silver ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_master.csv")
)

# ---------------------------------------------------------------------
# Target Table: gold_sales
# ---------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        DATE(sts.sold_date)                AS sold_date,
        CAST(sts.quantity AS INT)          AS quantity,
        CAST(sts.total_revenue AS DOUBLE)  AS total_revenue,
        CAST(ps.product_category AS STRING) AS product_category,
        CAST(ss.store_region AS STRING)     AS store_region
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales.csv")
)
