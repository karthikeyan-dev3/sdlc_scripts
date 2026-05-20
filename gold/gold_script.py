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
sales_store_product_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_store_product_daily_silver.{FILE_FORMAT}/")
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
# 2) Create temp views
# ----------------------------
sales_store_product_daily_silver_df.createOrReplaceTempView("sales_store_product_daily_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# TARGET TABLE: gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sspd.store_id AS STRING)                         AS store_id,
        DATE(sspd.reporting_date)                             AS reporting_date,
        CAST(ss.city AS STRING)                               AS city,
        CAST(ss.state AS STRING)                              AS state,
        CAST(ss.store_type AS STRING)                         AS store_type,
        CAST(SUM(CAST(sspd.revenue AS DOUBLE)) AS DOUBLE)     AS revenue,
        CAST(SUM(CAST(sspd.quantity_sold AS INT)) AS INT)     AS quantity_sold
    FROM sales_store_product_daily_silver sspd
    INNER JOIN stores_silver ss
        ON sspd.store_id = ss.store_id
    GROUP BY
        sspd.store_id,
        sspd.reporting_date,
        ss.city,
        ss.state,
        ss.store_type
    """
)

gold_store_performance_output = f"{TARGET_PATH}/gold_store_performance.csv"
(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_store_performance_output)
)

# ============================================================
# TARGET TABLE: gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sspd.product_id AS STRING)                       AS product_id,
        DATE(sspd.reporting_date)                             AS reporting_date,
        CAST(ps.product_name AS STRING)                       AS product_name,
        CAST(ps.category AS STRING)                           AS category,
        CAST(ps.brand AS STRING)                              AS brand,
        CAST(ps.price AS FLOAT)                               AS price,
        CAST(SUM(CAST(sspd.revenue AS DOUBLE)) AS DOUBLE)     AS revenue,
        CAST(SUM(CAST(sspd.quantity_sold AS INT)) AS INT)     AS quantity_sold
    FROM sales_store_product_daily_silver sspd
    INNER JOIN products_silver ps
        ON sspd.product_id = ps.product_id
    GROUP BY
        sspd.product_id,
        sspd.reporting_date,
        ps.product_name,
        ps.category,
        ps.brand,
        ps.price
    """
)

gold_product_performance_output = f"{TARGET_PATH}/gold_product_performance.csv"
(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_product_performance_output)
)

# ============================================================
# TARGET TABLE: gold_aggregated_sales
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sspd.store_id AS STRING)           AS store_id,
        CAST(sspd.product_id AS STRING)         AS product_id,
        DATE(sspd.reporting_date)               AS reporting_date,
        CAST(sspd.revenue AS DOUBLE)            AS revenue,
        CAST(sspd.quantity_sold AS INT)         AS quantity_sold,
        CAST(ss.city AS STRING)                 AS city,
        CAST(ss.state AS STRING)                AS state,
        CAST(ss.store_type AS STRING)           AS store_type,
        CAST(ps.product_name AS STRING)         AS product_name,
        CAST(ps.category AS STRING)             AS category,
        CAST(ps.brand AS STRING)                AS brand,
        CAST(ps.price AS FLOAT)                 AS price
    FROM sales_store_product_daily_silver sspd
    INNER JOIN stores_silver ss
        ON sspd.store_id = ss.store_id
    INNER JOIN products_silver ps
        ON sspd.product_id = ps.product_id
    """
)

gold_aggregated_sales_output = f"{TARGET_PATH}/gold_aggregated_sales.csv"
(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_aggregated_sales_output)
)

job.commit()