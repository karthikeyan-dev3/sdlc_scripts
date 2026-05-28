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

# -----------------------------
# Read Source Tables from S3
# -----------------------------
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

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# =========================================================
# Target Table: gold_sales_transactions
# =========================================================
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.total_revenue AS DOUBLE) AS total_revenue
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# =========================================================
# Target Table: gold_product_master
# =========================================================
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.product_category AS STRING) AS product_category,
  CAST(SUM(CAST(ass.total_revenue AS DOUBLE)) AS DOUBLE) AS revenue_contribution
FROM products_silver ps
LEFT JOIN aggregated_sales_silver ass
  ON ps.product_id = ass.product_id
GROUP BY
  ps.product_id,
  ps.product_name,
  ps.product_category
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# =========================================================
# Target Table: gold_store_master
# =========================================================
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.store_location AS STRING) AS store_location,
  CAST(SUM(CAST(ass.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue
FROM stores_silver ss
LEFT JOIN aggregated_sales_silver ass
  ON ss.store_id = ass.store_id
GROUP BY
  ss.store_id,
  ss.store_name,
  ss.store_location
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# =========================================================
# Target Table: gold_aggregated_sales
# =========================================================
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(ass.date AS DATE) AS date,
  CAST(ass.store_id AS STRING) AS store_id,
  CAST(ass.product_id AS STRING) AS product_id,
  CAST(ass.total_quantity_sold AS INT) AS total_quantity_sold,
  CAST(ass.total_revenue AS DOUBLE) AS total_revenue,
  CAST(ass.avg_revenue_per_transaction AS DOUBLE) AS avg_revenue_per_transaction
FROM aggregated_sales_silver ass
""")

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()