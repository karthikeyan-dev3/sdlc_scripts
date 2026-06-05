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

# ------------------------------------------------------------------------------
# Source Reads + Temp Views
# ------------------------------------------------------------------------------

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

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

# ------------------------------------------------------------------------------
# Target: gold_sales
# ------------------------------------------------------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        sts.sale_date AS sale_date,
        sts.quantity_sold AS quantity_sold,
        sts.revenue AS revenue,
        sts.transaction_count AS transaction_count
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
# Target: gold_product_master
# ------------------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        pms.brand AS brand,
        pms.price AS price
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
# Target: gold_store_master
# ------------------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.location AS location,
        sms.region AS region
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
# Target: gold_sales_aggregated
# ------------------------------------------------------------------------------

gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        sts.sale_date AS date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        SUM(sts.revenue) AS total_revenue,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.transaction_count) AS total_transaction_count
    FROM sales_transactions_silver sts
    GROUP BY
        sts.sale_date,
        sts.product_id,
        sts.store_id
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()