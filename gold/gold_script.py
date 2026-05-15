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
# 1) Read source tables from S3
# ------------------------------------------------------------------------------

sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL + 4) Write each target table separately
# ------------------------------------------------------------------------------

# --- gold_sales_performance (gsp) ---
gold_sales_performance_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  DATE(sts.transaction_time) AS transaction_date,
  CAST(sts.sale_amount AS DOUBLE) AS revenue,
  CAST(sts.quantity AS INT) AS quantity
FROM sales_transactions_silver sts
""")

(
    gold_sales_performance_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# --- gold_store_performance (gstorep) ---
gold_store_performance_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(DISTINCT sts.transaction_id) AS BIGINT) AS total_transactions,
  CAST(
    SUM(CAST(sts.sale_amount AS DOUBLE)) / COUNT(DISTINCT sts.transaction_id)
    AS DOUBLE
  ) AS average_transaction_value
FROM sales_transactions_silver sts
INNER JOIN store_master_silver sms
  ON sts.store_id = sms.store_id
GROUP BY sts.store_id
""")

(
    gold_store_performance_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# --- gold_product_performance (gprodp) ---
gold_product_performance_df = spark.sql("""
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(SUM(CAST(sts.quantity AS INT)) AS BIGINT) AS total_units_sold,
  CASE
    WHEN SUM(CAST(sts.sale_amount AS DOUBLE)) >= 100000 THEN 'top'
    WHEN SUM(CAST(sts.sale_amount AS DOUBLE)) >= 50000 THEN 'mid'
    ELSE 'low'
  END AS performance_category
FROM sales_transactions_silver sts
INNER JOIN product_master_silver pms
  ON sts.product_id = pms.product_id
GROUP BY sts.product_id
""")

(
    gold_product_performance_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# --- gold_product_master (gpm) ---
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING) AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING) AS category,
  CAST(pms.price AS FLOAT) AS price
FROM product_master_silver pms
""")

(
    gold_product_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# --- gold_store_master (gsm) ---
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING) AS store_id,
  CAST(sms.store_name AS STRING) AS store_name,
  CAST(sms.state AS STRING) AS region,
  CAST(sms.store_type AS STRING) AS store_type
FROM store_master_silver sms
""")

(
    gold_store_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

job.commit()