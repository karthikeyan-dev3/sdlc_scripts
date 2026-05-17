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

# ------------------------------------------------------------
# Read Source Tables
# ------------------------------------------------------------
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
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
transactions_silver_df.createOrReplaceTempView("transactions_silver")

# ------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------
gold_product_master_sql = """
SELECT
  CAST(ps.product_id AS STRING)  AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.price AS DOUBLE)        AS price
FROM products_silver ps
"""
gold_product_master_df = spark.sql(gold_product_master_sql)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------
gold_store_master_sql = """
SELECT
  CAST(ss.store_id AS STRING)   AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING)   AS location
FROM stores_silver ss
"""
gold_store_master_df = spark.sql(gold_store_master_sql)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------
# Target: gold_aggregated_sales
# ------------------------------------------------------------
gold_aggregated_sales_sql = """
SELECT
  CAST(ts.transaction_id AS STRING) AS transaction_id,
  DATE(CAST(ts.date AS STRING))     AS date,
  CAST(ts.store_id AS STRING)       AS store_id,
  CAST(ts.product_id AS STRING)     AS product_id,
  CAST(ts.quantity_sold AS INT)     AS quantity_sold,
  CAST(ts.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM transactions_silver ts
INNER JOIN products_silver ps
  ON ts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON ts.store_id = ss.store_id
"""
gold_aggregated_sales_df = spark.sql(gold_aggregated_sales_sql)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ------------------------------------------------------------
# Target: gold_sales_summary
# ------------------------------------------------------------
gold_sales_summary_sql = """
SELECT
  DATE(CAST(ts.date AS STRING)) AS date,
  CAST(SUM(CAST(ts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
  CAST(COUNT(CAST(ts.transaction_id AS STRING)) AS STRING) AS total_transactions,
  CAST(
    SUM(CAST(ts.total_sales_amount AS DOUBLE)) / COUNT(CAST(ts.transaction_id AS STRING))
    AS DOUBLE
  ) AS average_transaction_value
FROM transactions_silver ts
INNER JOIN products_silver ps
  ON ts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON ts.store_id = ss.store_id
GROUP BY DATE(CAST(ts.date AS STRING))
"""
gold_sales_summary_df = spark.sql(gold_sales_summary_sql)

(
    gold_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_summary.csv")
)

job.commit()