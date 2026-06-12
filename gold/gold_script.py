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

# ----------------------------
# Read Source Tables (Silver)
# ----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# =========================================================
# Target Table: gold_sales_txn_enriched
# =========================================================
gold_sales_txn_enriched_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS sales_txn_id,
        CAST(sts.transaction_time AS DATE) AS sales_date,
        sts.store_id AS store_id,
        ss.store_name AS store_name,
        ss.city AS store_city,
        ss.state AS store_state,
        ss.state AS store_region,
        sts.product_id AS product_id,
        ps.product_name AS product_name,
        ps.category AS product_category,
        ps.brand AS product_brand,
        CAST(sts.quantity AS INT) AS quantity_sold,
        CAST(ps.price AS DOUBLE) AS unit_price,
        CAST(sts.sale_amount AS DOUBLE) AS gross_amount,
        CAST(sts.sale_amount AS DOUBLE) AS net_amount,
        CAST('USD' AS STRING) AS currency_code
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_sales_txn_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_txn_enriched.csv")
)

# =========================================================
# Target Table: gold_store_daily_performance
# =========================================================
gold_store_daily_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE) AS sales_date,
        sts.store_id AS store_id,
        ss.store_name AS store_name,
        ss.state AS store_region,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
        COUNT(DISTINCT sts.transaction_id) AS transaction_count,
        SUM(CAST(sts.quantity AS BIGINT)) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        sts.store_id,
        ss.store_name,
        ss.state
    """
)

(
    gold_store_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_performance.csv")
)

# =========================================================
# Target Table: gold_product_daily_performance
# =========================================================
gold_product_daily_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE) AS sales_date,
        sts.product_id AS product_id,
        ps.product_name AS product_name,
        ps.category AS product_category,
        ps.brand AS product_brand,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
        COUNT(DISTINCT sts.transaction_id) AS transaction_count,
        SUM(CAST(sts.quantity AS BIGINT)) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        sts.product_id,
        ps.product_name,
        ps.category,
        ps.brand
    """
)

(
    gold_product_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_performance.csv")
)

job.commit()
