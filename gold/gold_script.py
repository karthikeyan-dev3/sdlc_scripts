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
# 1) Read source tables from S3
# ------------------------------------------------------------
sales_transaction_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transaction_silver.{FILE_FORMAT}/")
)

product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

performance_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/performance_aggregates_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transaction_silver_df.createOrReplaceTempView("sales_transaction_silver")
product_silver_df.createOrReplaceTempView("product_silver")
store_silver_df.createOrReplaceTempView("store_silver")
performance_aggregates_silver_df.createOrReplaceTempView("performance_aggregates_silver")

# ------------------------------------------------------------
# 3) Transform + 4) Save output (gold_sales)
# Mapping: silver.sales_transaction_silver sts
#          LEFT JOIN silver.product_silver ps ON sts.product_id = ps.product_id
#          LEFT JOIN silver.store_silver ss ON sts.store_id = ss.store_id
# Columns per UDT
# ------------------------------------------------------------
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)      AS transaction_id,
  CAST(sts.product_id AS STRING)          AS product_id,
  CAST(sts.store_id AS STRING)            AS store_id,
  CAST(sts.transaction_date AS DATE)      AS transaction_date,
  CAST(sts.quantity_sold AS INT)          AS quantity_sold,
  CAST(sts.revenue AS DOUBLE)             AS revenue
FROM sales_transaction_silver sts
LEFT JOIN product_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN store_silver ss
  ON sts.store_id = ss.store_id
""")

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ------------------------------------------------------------
# 3) Transform + 4) Save output (gold_product)
# Mapping: silver.product_silver ps
# Columns per UDT
# ------------------------------------------------------------
gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)       AS product_id,
  CAST(ps.product_name AS STRING)     AS product_name,
  CAST(ps.category AS STRING)         AS category,
  CAST(ps.price AS DOUBLE)            AS price
FROM product_silver ps
""")

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ------------------------------------------------------------
# 3) Transform + 4) Save output (gold_store)
# Mapping: silver.store_silver ss
# Columns per UDT
# ------------------------------------------------------------
gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)       AS store_id,
  CAST(ss.store_name AS STRING)     AS store_name,
  CAST(ss.region AS STRING)         AS region
FROM store_silver ss
""")

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ------------------------------------------------------------
# 3) Transform + 4) Save output (gold_performance_analytics)
# Mapping: silver.performance_aggregates_silver pas
#          LEFT JOIN silver.store_silver ss ON pas.store_id = ss.store_id
#          LEFT JOIN silver.product_silver ps ON pas.product_id = ps.product_id
# Columns per UDT
# ------------------------------------------------------------
gold_performance_analytics_df = spark.sql("""
SELECT
  CAST(pas.store_id AS STRING)                 AS store_id,
  CAST(pas.product_id AS STRING)               AS product_id,
  CAST(pas.date AS DATE)                       AS date,
  CAST(pas.total_revenue AS DOUBLE)            AS total_revenue,
  CAST(pas.total_transactions AS INT)          AS total_transactions,
  CAST(pas.total_quantity_sold AS INT)         AS total_quantity_sold,
  CAST(pas.average_price AS DOUBLE)            AS average_price
FROM performance_aggregates_silver pas
LEFT JOIN store_silver ss
  ON pas.store_id = ss.store_id
LEFT JOIN product_silver ps
  ON pas.product_id = ps.product_id
""")

(
    gold_performance_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_performance_analytics.csv")
)

job.commit()