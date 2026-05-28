import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------
# Target: gold_product_details
# ------------------------------------------------------------
gold_product_details_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS price
    FROM products_silver ps
    """
)

(
    gold_product_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_details.csv")
)

# ------------------------------------------------------------
# Target: gold_store_details
# ------------------------------------------------------------
gold_store_details_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)        AS store_id,
        CAST(ss.store_location AS STRING)  AS store_location,
        CAST(ss.store_type AS STRING)      AS store_type
    FROM stores_silver ss
    """
)

(
    gold_store_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_details.csv")
)

# ------------------------------------------------------------
# Target: gold_sales_performance
# ------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.revenue AS DOUBLE)        AS revenue,
        DATE(CAST(sts.transaction_date AS STRING)) AS transaction_date,
        CAST(sts.quantity_sold AS INT)     AS quantity_sold,
        CAST(ps.category AS STRING)        AS category
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

job.commit()
