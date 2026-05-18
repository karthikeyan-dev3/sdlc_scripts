import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

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

# ----------------------------
# Create temp views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: gold.gold_sales_transactions
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.date) AS date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.cleaned AS BOOLEAN) AS cleaned,
        CAST(sts.validated AS BOOLEAN) AS validated
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

# ============================================================
# Target: gold.gold_product_enriched_sales
# ============================================================
gold_product_enriched_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.date) AS date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.brand AS STRING) AS brand,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_product_enriched_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_enriched_sales.csv")
)

# ============================================================
# Target: gold.gold_store_enriched_sales
# ============================================================
gold_store_enriched_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.date) AS date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

(
    gold_store_enriched_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_enriched_sales.csv")
)

# ============================================================
# Target: gold.gold_aggregated_sales
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(sts.date) AS date,
        CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
        CAST(COUNT(sts.transaction_id) AS INT) AS total_transactions,
        CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) / COUNT(sts.transaction_id) AS DOUBLE) AS avg_sales_per_transaction,
        CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE) AS product_category_sales_amount,
        CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE) AS store_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        DATE(sts.date)
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)