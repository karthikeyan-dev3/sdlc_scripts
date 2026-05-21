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

# ------------------------------------------------------------------------------
# 1) READ SOURCE TABLES (S3) + TEMP VIEWS
# ------------------------------------------------------------------------------

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# 2) TARGET: gold_product_master
# ------------------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)      AS product_id,
        CAST(pms.product_name AS STRING)    AS product_name,
        CAST(pms.category AS STRING)        AS category,
        CAST(pms.price AS FLOAT)            AS price
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------------------------
# 3) TARGET: gold_store_master
# ------------------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)     AS store_id,
        CAST(sms.store_name AS STRING)   AS store_name,
        CAST(sms.location AS STRING)     AS location,
        CAST(sms.region AS STRING)       AS region
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------------------------
# 4) TARGET: gold_sales
# ------------------------------------------------------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)   AS transaction_id,
        CAST(sts.product_id AS STRING)       AS product_id,
        CAST(sts.store_id AS STRING)         AS store_id,
        CAST(sts.sale_date AS DATE)          AS sale_date,
        CAST(sts.quantity_sold AS INT)       AS quantity_sold,
        CAST(sts.total_sales AS DOUBLE)      AS total_sales
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

# ------------------------------------------------------------------------------
# 5) TARGET: gold_aggregated_sales
# ------------------------------------------------------------------------------

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sale_date AS DATE)                                AS date,
        CAST(sms.region AS STRING)                                 AS region,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)            AS total_quantity_sold,
        CAST(SUM(CAST(sts.total_sales AS DOUBLE)) AS DOUBLE)        AS total_revenue,
        CAST(COUNT(DISTINCT sts.product_id) AS INT)                 AS unique_products_sold,
        CAST(COUNT(DISTINCT sts.store_id) AS INT)                   AS unique_stores
    FROM sales_transactions_silver sts
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY
        CAST(sts.sale_date AS DATE),
        CAST(sms.region AS STRING)
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