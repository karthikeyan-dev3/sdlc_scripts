import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------
# Source Read: silver.sales_store_day_silver (CSV)
# ---------------------------------------------------------------------
ssds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_store_day_silver.{FILE_FORMAT}/")
)
ssds_df.createOrReplaceTempView("sales_store_day_silver")

# ---------------------------------------------------------------------
# Target: gold.gold_sales_performance
# ---------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(ssds.store_id AS STRING) AS store_id,
        DATE(ssds.transaction_date) AS transaction_date,
        CAST(ssds.total_revenue AS DOUBLE) AS total_revenue,
        CAST(ssds.transaction_count AS BIGINT) AS transaction_count,
        CAST(ssds.quantities_sold AS BIGINT) AS quantities_sold,
        DATE(ssds.data_refresh_date) AS data_refresh_date
    FROM sales_store_day_silver ssds
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ---------------------------------------------------------------------
# Source Read: silver.sales_product_day_silver (CSV)
# ---------------------------------------------------------------------
spds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_product_day_silver.{FILE_FORMAT}/")
)
spds_df.createOrReplaceTempView("sales_product_day_silver")

# ---------------------------------------------------------------------
# Target: gold.gold_product_performance
# ---------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(spds.product_id AS STRING) AS product_id,
        spds.category_id AS category_id,
        CAST(spds.product_revenue AS DOUBLE) AS revenue_contribution,
        CAST(spds.quantities_sold AS BIGINT) AS quantities_sold,
        DATE(spds.data_refresh_date) AS data_refresh_date
    FROM sales_product_day_silver spds
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()
