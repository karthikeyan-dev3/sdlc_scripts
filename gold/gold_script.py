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

# --------------------------------------------------------------------
# 1) READ SOURCE TABLES (S3) + TEMP VIEWS
# --------------------------------------------------------------------
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)
transactions_silver_df.createOrReplaceTempView("transactions_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

# --------------------------------------------------------------------
# TARGET TABLE: gold_sales_analytics
# --------------------------------------------------------------------
gold_sales_analytics_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(ts.transaction_id AS STRING)           AS transaction_id,
            CAST(ts.store_id AS STRING)                 AS store_id,
            CAST(ts.product_id AS STRING)               AS product_id,
            CAST(ts.transaction_date AS DATE)           AS transaction_date,
            CAST(ts.quantity_sold AS INT)               AS quantity_sold,
            CAST(ts.total_revenue AS DOUBLE)            AS total_revenue,
            CAST(ts.price_per_unit AS DOUBLE)           AS price_per_unit,
            ROW_NUMBER() OVER (
                PARTITION BY ts.transaction_id, ts.store_id, ts.product_id
                ORDER BY ts.transaction_date DESC
            ) AS rn
        FROM transactions_silver ts
        INNER JOIN stores_silver ss
            ON ts.store_id = ss.store_id
        INNER JOIN products_silver ps
            ON ts.product_id = ps.product_id
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        total_revenue,
        price_per_unit
    FROM base
    WHERE rn = 1
    """
)

(
    gold_sales_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_analytics.csv")
)

# --------------------------------------------------------------------
# TARGET TABLE: gold_store_performance
# --------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(SUM(CAST(ts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)) AS BIGINT) AS num_transactions,
        CAST(
            SUM(CAST(ts.total_revenue AS DOUBLE)) /
            NULLIF(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)), 0)
            AS DOUBLE
        ) AS average_transaction_value
    FROM transactions_silver ts
    INNER JOIN stores_silver ss
        ON ts.store_id = ss.store_id
    GROUP BY
        CAST(ts.store_id AS STRING)
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_performance.csv")
)

# --------------------------------------------------------------------
# TARGET TABLE: gold_product_performance
# --------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ts.product_id AS STRING) AS product_id,
        CAST(SUM(CAST(ts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(ts.quantity_sold AS BIGINT)) AS BIGINT) AS units_sold,
        CAST(
            SUM(CAST(ts.total_revenue AS DOUBLE)) /
            NULLIF(SUM(CAST(ts.quantity_sold AS BIGINT)), 0)
            AS DOUBLE
        ) AS average_price
    FROM transactions_silver ts
    INNER JOIN products_silver ps
        ON ts.product_id = ps.product_id
    GROUP BY
        CAST(ts.product_id AS STRING)
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()