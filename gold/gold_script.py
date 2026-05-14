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

# -------------------------------------------------------------------
# Read source tables from S3
# -------------------------------------------------------------------
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

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

data_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create temp views
# -------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

# -------------------------------------------------------------------
# gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)  AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS price
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

# -------------------------------------------------------------------
# gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)    AS store_id,
        CAST(ss.store_name AS STRING)  AS store_name,
        CAST(ss.region AS STRING)      AS region
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

# -------------------------------------------------------------------
# gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)            AS transaction_id,
        DATE(CAST(sts.transaction_date AS DATE))      AS transaction_date,
        CAST(sts.store_id AS STRING)                  AS store_id,
        CAST(sts.product_id AS STRING)                AS product_id,
        CAST(sts.quantity_sold AS INT)                AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE)             AS total_revenue
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

# -------------------------------------------------------------------
# gold_sales_aggregated
# -------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(CAST(sas.date AS DATE))        AS date,
        CAST(sas.total_revenue AS DOUBLE)   AS total_revenue,
        CAST(sas.quantity_sold AS INT)      AS quantity_sold,
        CAST(sas.store_id AS STRING)        AS store_id,
        CAST(sas.product_id AS STRING)      AS product_id,
        CAST(sas.category AS STRING)        AS category
    FROM sales_aggregated_silver sas
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# -------------------------------------------------------------------
# gold_data_quality
# -------------------------------------------------------------------
gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqs.data_check AS STRING)          AS data_check,
        DATE(CAST(dqs.check_date AS DATE))      AS check_date,
        CAST(dqs.passed AS BOOLEAN)             AS passed,
        CAST(dqs.errors_count AS INT)           AS errors_count
    FROM data_quality_silver dqs
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()