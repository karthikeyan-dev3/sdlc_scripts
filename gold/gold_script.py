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

# =========================================================
# Read source tables (S3) + Create temp views
# =========================================================

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_silver.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("sales_performance_silver")

dpms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_product_metrics_silver.{FILE_FORMAT}/")
)
dpms_df.createOrReplaceTempView("daily_product_metrics_silver")

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("products_silver")

dsms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_store_metrics_silver.{FILE_FORMAT}/")
)
dsms_df.createOrReplaceTempView("daily_store_metrics_silver")

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("stores_silver")

dsss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_summary_silver.{FILE_FORMAT}/")
)
dsss_df.createOrReplaceTempView("daily_sales_summary_silver")

# =========================================================
# Target: gold_sales_performance
# =========================================================

gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.transaction_id AS STRING)           AS transaction_id,
        DATE(sps.transaction_date)                  AS transaction_date,
        CAST(sps.store_id AS STRING)                AS store_id,
        CAST(sps.store_name AS STRING)              AS store_name,
        CAST(sps.product_id AS STRING)              AS product_id,
        CAST(sps.product_name AS STRING)            AS product_name,
        CAST(sps.sales_amount AS DOUBLE)            AS sales_amount,
        CAST(sps.quantity_sold AS INT)              AS quantity_sold,
        CAST(sps.region AS STRING)                  AS region,
        CAST(sps.category AS STRING)                AS category
    FROM sales_performance_silver sps
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# =========================================================
# Target: gold_product_performance
# =========================================================

gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(dpms.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(SUM(CAST(dpms.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount,
        CAST(SUM(CAST(dpms.total_quantity_sold AS INT)) AS INT)      AS total_quantity_sold,
        CAST(
            SUM(CAST(dpms.total_sales_amount AS DOUBLE)) /
            NULLIF(SUM(CAST(dpms.number_of_transactions AS BIGINT)), 0)
            AS DOUBLE
        ) AS average_transaction_value
    FROM daily_product_metrics_silver dpms
    INNER JOIN products_silver ps
        ON dpms.product_id = ps.product_id
    GROUP BY
        dpms.product_id,
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

# =========================================================
# Target: gold_store_performance
# =========================================================

gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(dsms.store_id AS STRING)   AS store_id,
        CAST(ss.store_name AS STRING)  AS store_name,
        CAST(ss.region AS STRING)      AS region,
        CAST(SUM(CAST(dsms.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount,
        CAST(SUM(CAST(dsms.total_quantity_sold AS INT)) AS INT)      AS total_quantity_sold,
        CAST(SUM(CAST(dsms.number_of_transactions AS BIGINT)) AS BIGINT) AS number_of_transactions,
        CAST(
            SUM(CAST(dsms.total_quantity_sold AS INT)) /
            NULLIF(SUM(CAST(dsms.number_of_transactions AS BIGINT)), 0)
            AS DOUBLE
        ) AS average_transaction_size
    FROM daily_store_metrics_silver dsms
    INNER JOIN stores_silver ss
        ON dsms.store_id = ss.store_id
    GROUP BY
        dsms.store_id,
        ss.store_name,
        ss.region
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# =========================================================
# Target: gold_aggregated_sales
# =========================================================

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(dsss.date)                            AS date,
        CAST(dsss.total_sales_amount AS DOUBLE)    AS total_sales_amount,
        CAST(dsss.total_quantity_sold AS INT)      AS total_quantity_sold,
        CAST(dsss.number_of_transactions AS BIGINT) AS number_of_transactions,
        CAST(dsss.total_stores AS BIGINT)          AS total_stores,
        CAST(
            CAST(dsss.total_sales_amount AS DOUBLE) /
            NULLIF(CAST(dsss.total_stores AS BIGINT), 0)
            AS DOUBLE
        ) AS average_sales_per_store
    FROM daily_sales_summary_silver dsss
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
