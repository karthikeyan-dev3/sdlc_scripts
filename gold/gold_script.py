import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# -------------------------
# 1) Read source tables
# -------------------------
sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# -------------------------
# 2) Create temp views
# -------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------
# 3) Transform + 4) Write: gold_sales
# -------------------------
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)        AS transaction_id,
  CAST(sts.product_id AS STRING)            AS product_id,
  CAST(sts.store_id AS STRING)              AS store_id,
  CAST(sts.transaction_date AS DATE)        AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE)          AS sales_amount,
  CAST(sts.quantity_sold AS INT)            AS quantity_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -------------------------
# 3) Transform + 4) Write: gold_product_master
# -------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)         AS product_id,
  CAST(pms.product_name AS STRING)       AS product_name,
  CAST(pms.product_category AS STRING)   AS product_category,
  CAST(pms.price AS DOUBLE)              AS price
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

# -------------------------
# 3) Transform + 4) Write: gold_store_master
# -------------------------
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)          AS store_id,
  CAST(sms.store_name AS STRING)        AS store_name,
  CAST(sms.store_location AS STRING)    AS store_location,
  CAST(sms.store_region AS STRING)      AS store_region
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

# -------------------------
# 3) Transform + 4) Write: gold_sales_aggregated
# -------------------------
gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING)                 AS store_id,
  CAST(sts.product_id AS STRING)               AS product_id,
  CAST(sts.transaction_date AS DATE)           AS reporting_date,
  CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE)          AS total_sales_amount,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)               AS total_quantity_sold,
  CAST(AVG(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE)          AS average_transaction_value
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.transaction_date
""")

(
    gold_sales_aggregated_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()