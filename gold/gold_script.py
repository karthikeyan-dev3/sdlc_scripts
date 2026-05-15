from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)   AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING)     AS category,
        CAST(pms.price AS DOUBLE)        AS price
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

# ------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)     AS store_id,
        CAST(sms.store_name AS STRING)   AS store_name,
        CAST(sms.state AS STRING)        AS region,
        CAST(sms.store_type AS STRING)   AS store_type
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

# ------------------------------------------------------------
# Target: gold_sales
# ------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)              AS sales_id,
        CAST(DATE(sts.transaction_time) AS DATE)        AS transaction_date,
        CAST(pms.product_id AS STRING)                  AS product_id,
        CAST(sms.store_id AS STRING)                    AS store_id,
        CAST(sts.sale_amount AS DOUBLE)                 AS revenue,
        CAST(sts.quantity AS INT)                       AS quantity_sold,
        CAST(pms.category AS STRING)                    AS category
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ------------------------------------------------------------
# Target: gold_aggregated_sales
# ------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sas.transaction_date AS DATE)  AS date,
        CAST(sas.store_id AS STRING)        AS store_id,
        CAST(sas.revenue AS DOUBLE)         AS total_revenue,
        CAST(sas.sales_id AS BIGINT)        AS total_transactions,
        CAST(sas.quantity_sold AS BIGINT)   AS total_quantity_sold
    FROM aggregated_sales_silver sas
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