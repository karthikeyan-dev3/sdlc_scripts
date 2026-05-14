
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------
# Read Source Tables from S3 (Silver)
# ------------------------------------------------------------------------------

sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

product_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
product_silver_df.createOrReplaceTempView("product_silver")

store_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
store_silver_df.createOrReplaceTempView("store_silver")

aggregated_sales_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ------------------------------------------------------------------------------
# Target: gold_sales
# Source: silver.sales_transactions_silver sts
# ------------------------------------------------------------------------------

gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.sales_date AS DATE) AS sales_date,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.revenue AS DOUBLE) AS revenue
FROM sales_transactions_silver sts
""")

(
    gold_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_sales.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_product
# Source: silver.product_silver ps
# ------------------------------------------------------------------------------

gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.price AS FLOAT) AS price
FROM product_silver ps
""")

(
    gold_product_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_product.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_store
# Source: silver.store_silver ss
# ------------------------------------------------------------------------------

gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location
FROM store_silver ss
""")

(
    gold_store_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_store.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_aggregated_sales
# Source: silver.aggregated_sales_silver sas
# ------------------------------------------------------------------------------

gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(sas.store_id AS STRING) AS store_id,
  CAST(sas.sales_date AS DATE) AS sales_date,
  CAST(sas.total_revenue AS DOUBLE) AS total_revenue,
  CAST(sas.total_transactions AS BIGINT) AS total_transactions,
  CAST(sas.total_quantity AS BIGINT) AS total_quantity
FROM aggregated_sales_silver sas
""")

(
    gold_aggregated_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_aggregated_sales.csv")
)

job.commit()
