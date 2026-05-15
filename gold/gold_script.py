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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# --------------------------------------------------------------------------------------
# 1) READ SOURCE TABLES FROM S3
# --------------------------------------------------------------------------------------
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

daily_sales_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_summary_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) CREATE TEMP VIEWS
# --------------------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

# --------------------------------------------------------------------------------------
# TARGET: gold_sales_transactions
# --------------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        DATE(sts.sale_date) AS sale_date,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue
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

# --------------------------------------------------------------------------------------
# TARGET: gold_products
# --------------------------------------------------------------------------------------
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.price AS FLOAT) AS price,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS quantity_sold,
        CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS revenue_contribution
    FROM products_silver ps
    LEFT JOIN sales_transactions_silver sts
        ON ps.product_id = sts.product_id
    GROUP BY
        ps.product_id,
        ps.product_name,
        ps.category,
        ps.price
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: gold_stores
# --------------------------------------------------------------------------------------
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS BIGINT) AS transaction_count
    FROM stores_silver ss
    LEFT JOIN sales_transactions_silver sts
        ON ss.store_id = sts.store_id
    GROUP BY
        ss.store_id,
        ss.store_name,
        ss.location,
        ss.store_type
    """
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: gold_reports_summary
# --------------------------------------------------------------------------------------
gold_reports_summary_df = spark.sql(
    """
    SELECT
        DATE(dss.date) AS date,
        CAST(dss.total_revenue AS DOUBLE) AS total_revenue,
        CAST(dss.total_transactions AS BIGINT) AS total_transactions,
        CAST(dss.top_product_id AS STRING) AS top_product_id,
        CAST(ps.product_name AS STRING) AS top_product_name,
        CAST(dss.top_store_id AS STRING) AS top_store_id,
        CAST(ss.store_name AS STRING) AS top_store_name
    FROM daily_sales_summary_silver dss
    LEFT JOIN products_silver ps
        ON dss.top_product_id = ps.product_id
    LEFT JOIN stores_silver ss
        ON dss.top_store_id = ss.store_id
    """
)

(
    gold_reports_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_reports_summary.csv")
)

job.commit()