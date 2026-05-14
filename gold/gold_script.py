
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

# ----------------------------
# 1) Read source tables from S3
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
# 2) Create temp views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: gold_sales_transactions
# Mapping: silver.sales_transactions_silver sts
#          LEFT JOIN silver.products_silver ps ON sts.product_id = ps.product_id
#          LEFT JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.date) AS date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.total_amount AS DOUBLE) AS total_amount
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
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
# Target: gold_store_performance
# Mapping: silver.sales_transactions_silver sts
#          INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        CAST(SUM(CAST(sts.total_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(sts.transaction_id) AS BIGINT) AS transaction_count
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        ss.store_id,
        ss.store_name,
        ss.location
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold_product_performance
# Mapping: silver.sales_transactions_silver sts
#          INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(SUM(CAST(sts.total_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS BIGINT) AS units_sold
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        ps.product_id,
        ps.product_name,
        ps.category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_product_store
# Mapping: silver.products_silver ps CROSS JOIN silver.stores_silver ss
# ============================================================
gold_product_store_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ss.store_id AS STRING) AS store_id
    FROM products_silver ps
    CROSS JOIN stores_silver ss
    """
)

(
    gold_product_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_store.csv")
)

job.commit()