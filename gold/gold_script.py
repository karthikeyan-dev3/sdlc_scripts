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

# -----------------------------
# Read Source Tables (Silver)
# -----------------------------
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

# -----------------------------
# Target: gold_product_master
# -----------------------------
gold_product_master_sql = """
SELECT
  CAST(ps.product_id AS STRING)       AS product_id,
  CAST(ps.product_name AS STRING)     AS product_name,
  CAST(ps.category AS STRING)         AS category,
  CAST(ps.price AS DOUBLE)            AS price
FROM products_silver ps
"""

gold_product_master_df = spark.sql(gold_product_master_sql)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------
# Target: gold_store_master
# -----------------------------
gold_store_master_sql = """
SELECT
  CAST(ss.store_id AS STRING)     AS store_id,
  CAST(ss.store_name AS STRING)   AS store_name,
  CAST(ss.region AS STRING)       AS region,
  CAST(ss.store_type AS STRING)   AS store_type
FROM stores_silver ss
"""

gold_store_master_df = spark.sql(gold_store_master_sql)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------
# Target: gold_sales_transactions
# -----------------------------
gold_sales_transactions_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)  AS transaction_id,
  DATE(CAST(sts.date AS STRING))      AS date,
  CAST(sts.store_id AS STRING)        AS store_id,
  CAST(sts.product_id AS STRING)      AS product_id,
  CAST(sts.quantity AS INT)           AS quantity,
  CAST(sts.revenue AS DOUBLE)         AS revenue
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""

gold_sales_transactions_df = spark.sql(gold_sales_transactions_sql)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

job.commit()
