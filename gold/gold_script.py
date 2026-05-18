import sys
from awsglue.context import GlueContext
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# Read Source Tables (S3)
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
# Create Temp Views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target Table: gold_sales
# ============================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        CAST(sts.revenue AS DECIMAL(38,18)) AS revenue,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
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
# Target Table: gold_products
# ============================================================
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(SUM(CAST(sts.revenue AS DECIMAL(38,18))) AS DECIMAL(38,18)) AS revenue_contribution,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
    FROM products_silver ps
    LEFT JOIN sales_transactions_silver sts
        ON ps.product_id = sts.product_id
    GROUP BY
        ps.product_id,
        ps.product_name,
        ps.category
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
# ============================================================
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        CAST(SUM(CAST(sts.revenue AS DECIMAL(38,18))) AS DECIMAL(38,18)) AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT) AS transaction_count,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
    FROM stores_silver ss
    LEFT JOIN sales_transactions_silver sts
        ON ss.store_id = sts.store_id
    GROUP BY
        ss.store_id,
        ss.store_name,
        ss.location
    """
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)