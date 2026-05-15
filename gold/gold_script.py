import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# ------------------------------------------------------------------
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

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ------------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)      AS product_id,
        CAST(pms.product_name AS STRING)    AS product_name,
        CAST(pms.category AS STRING)        AS category,
        CAST(pms.brand AS STRING)           AS brand,
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

# ------------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------------
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

# ------------------------------------------------------------------
# Target: gold_sales_transactions
# ------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)  AS transaction_id,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.store_id AS STRING)        AS store_id,
        DATE(sts.sale_date)                 AS sale_date,
        CAST(sts.quantity_sold AS INT)      AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
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

# ------------------------------------------------------------------
# Target: gold_aggregated_sales
# ------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(ass.report_date)                 AS report_date,
        CAST(ass.product_id AS STRING)        AS product_id,
        CAST(ass.store_id AS STRING)          AS store_id,
        CAST(ass.total_quantity_sold AS INT)  AS total_quantity_sold,
        CAST(ass.total_revenue AS DOUBLE)     AS total_revenue
    FROM aggregated_sales_silver ass
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
