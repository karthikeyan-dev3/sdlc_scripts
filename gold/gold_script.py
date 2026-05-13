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

# -------------------------------------
# 1) Read source tables from S3
# -------------------------------------
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

# -------------------------------------
# 2) Create temp views
# -------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# -------------------------------------
# Target: gold_sales_transactions
# -------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)     AS transaction_id,
        CAST(sts.product_id AS STRING)         AS product_id,
        CAST(sts.store_id AS STRING)           AS store_id,
        CAST(sts.quantity AS INT)              AS quantity,
        CAST(sts.revenue AS DOUBLE)            AS revenue,
        CAST(sts.transaction_date AS DATE)     AS transaction_date
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

# -------------------------------------
# Target: gold_product_master
# -------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)      AS product_id,
        CAST(ps.product_name AS STRING)    AS product_name,
        CAST(ps.category AS STRING)        AS category,
        CAST(ps.price AS FLOAT)            AS price
    FROM products_silver ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------
# Target: gold_store_master
# -------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)        AS store_id,
        CAST(ss.store_name AS STRING)      AS store_name,
        CAST(ss.location AS STRING)        AS location
    FROM stores_silver ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------
# Target: gold_aggregated_sales
# -------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_date AS DATE)                 AS date,
        CAST(SUM(sts.revenue) AS DOUBLE)                   AS total_revenue,
        CAST(SUM(sts.quantity) AS INT)                     AS total_quantity,
        CAST(SUM(sts.revenue) AS DOUBLE)                   AS category_revenue,
        CAST(SUM(sts.revenue) AS DOUBLE)                   AS store_revenue
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        sts.transaction_date,
        ps.category,
        ss.store_id
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
