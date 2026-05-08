import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ---------------------------
# Source: silver.sales_transactions_silver (sts)
# ---------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ---------------------------
# Target: gold.sales_transactions_gold
# ---------------------------
sales_transactions_gold_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)       AS transaction_id,
        CAST(sts.store_id AS STRING)             AS store_id,
        CAST(sts.product_id AS STRING)           AS product_id,
        CAST(sts.transaction_date AS DATE)       AS transaction_date,
        CAST(sts.quantity_sold AS INT)           AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE)        AS total_revenue,
        CAST(sts.price_per_unit AS DOUBLE)       AS price_per_unit
    FROM sales_transactions_silver sts
    """
)

(
    sales_transactions_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_gold.csv")
)

job.commit()
