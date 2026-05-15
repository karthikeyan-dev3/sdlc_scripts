import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ---------------------------------------------------------------------
# Read source tables from S3 (CSV) + create temp views
# ---------------------------------------------------------------------
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

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ---------------------------------------------------------------------
# Target: gold.gold_products
# Source: silver.products_silver ps
# ---------------------------------------------------------------------
gold_products_df = spark.sql(
    """
    SELECT
        ps.product_id   AS product_id,
        ps.product_name AS product_name,
        ps.category     AS category,
        CAST(ps.price AS FLOAT) AS price
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

# ---------------------------------------------------------------------
# Target: gold.gold_stores
# Source: silver.stores_silver ss
# ---------------------------------------------------------------------
gold_stores_df = spark.sql(
    """
    SELECT
        ss.store_id      AS store_id,
        ss.store_name    AS store_name,
        ss.region        AS region,
        ss.store_manager AS store_manager
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

# ---------------------------------------------------------------------
# Target: gold.gold_sales_transactions
# Source: silver.sales_transactions_silver sts
# ---------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        sts.transaction_id   AS transaction_id,
        sts.product_id       AS product_id,
        sts.store_id         AS store_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.total_amount AS DOUBLE) AS total_amount,
        CAST(sts.quantity AS INT) AS quantity
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

# ---------------------------------------------------------------------
# Target: gold.gold_aggregated_sales
# Source: silver.aggregated_sales_silver sas
# ---------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        sas.store_id AS store_id,
        sas.product_id AS product_id,
        CAST(sas.transaction_date AS DATE) AS transaction_date,
        CAST(sas.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.transaction_count AS BIGINT) AS transaction_count
    FROM aggregated_sales_silver sas
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