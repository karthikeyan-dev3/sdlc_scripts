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

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------
# 3) Transformations (Spark SQL) + 4) Save outputs
# ------------------------------------------------------------

# ---- gold.gold_sales_data ----
gold_sales_data_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)         AS transaction_id,
        CAST(sts.store_id AS STRING)               AS store_id,
        CAST(sts.product_id AS STRING)             AS product_id,
        CAST(sts.transaction_date AS DATE)         AS transaction_date,
        CAST(sts.quantity_sold AS INT)             AS quantity_sold,
        CAST(sts.revenue AS DOUBLE)                AS revenue
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_data.csv")
)

# ---- gold.gold_store_performance ----
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)                AS store_id,
        CAST(ss.store_name AS STRING)              AS store_name,
        CAST(ss.location AS STRING)                AS location,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE)                 AS total_revenue,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS total_transactions
    FROM stores_silver ss
    INNER JOIN sales_transactions_silver sts
        ON ss.store_id = sts.store_id
    GROUP BY
        ss.store_id,
        ss.store_name,
        ss.location
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ---- gold.gold_product_performance ----
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)              AS product_id,
        CAST(ps.product_name AS STRING)            AS product_name,
        CAST(ps.category AS STRING)                AS category,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS BIGINT)              AS total_quantity_sold,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE)                 AS revenue_contribution
    FROM products_silver ps
    INNER JOIN sales_transactions_silver sts
        ON ps.product_id = sts.product_id
    GROUP BY
        ps.product_id,
        ps.product_name,
        ps.category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()
