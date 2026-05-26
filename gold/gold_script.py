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

# -------------------------------------------------------------------
# Read source tables from S3
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# Create temp views
# -------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  DATE(sts.transaction_date) AS transaction_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.total_revenue AS DOUBLE) AS total_revenue,
  CAST(sts.payment_type AS STRING) AS payment_type
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_performance
# -------------------------------------------------------------------
gold_store_performance_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(sts.transaction_id) AS INT) AS transaction_count,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) / COUNT(sts.transaction_id) AS DOUBLE) AS average_transaction_value,
  DATE(sts.transaction_date) AS date
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.transaction_date
""")

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_performance
# -------------------------------------------------------------------
gold_product_performance_df = spark.sql("""
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS units_sold,
  CAST(
    CASE
      WHEN SUM(CAST(sts.quantity_sold AS INT)) > 0
      THEN SUM(CAST(sts.total_revenue AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS INT))
      ELSE NULL
    END
    AS DOUBLE
  ) AS average_price,
  DATE(sts.transaction_date) AS date
FROM sales_transactions_silver sts
GROUP BY
  sts.product_id,
  sts.transaction_date
""")

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.brand AS STRING) AS brand,
  CAST(ps.price AS FLOAT) AS price
FROM products_silver ps
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.region AS STRING) AS region,
  CAST(ss.store_type AS STRING) AS store_type,
  DATE(ss.open_date) AS open_date
FROM stores_silver ss
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

job.commit()