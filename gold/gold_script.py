
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
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

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

aggregated_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_daily_silver.{FILE_FORMAT}/")
)

daily_refresh_log_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_refresh_log_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")
daily_refresh_log_silver_df.createOrReplaceTempView("daily_refresh_log_silver")

# -------------------------------------------------------------------
# 3) Transformations using Spark SQL
# -------------------------------------------------------------------

# gold.gold_product_master
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)   AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.brand AS STRING)        AS brand,
  CAST(ps.price AS FLOAT)         AS price
FROM products_silver ps
""")

# gold.gold_store_master
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)     AS store_id,
  CAST(ss.store_name AS STRING)   AS store_name,
  CAST(ss.location AS STRING)     AS location,
  CAST(ss.store_manager AS STRING) AS store_manager
FROM stores_silver ss
""")

# gold.gold_sales_transactions
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING)     AS product_id,
  CAST(sts.store_id AS STRING)       AS store_id,
  DATE(sts.transaction_date)         AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE)   AS sales_amount,
  CAST(sts.quantity_sold AS INT)     AS quantity_sold
FROM sales_transactions_silver sts
""")

# gold.gold_aggregated_sales
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(asds.store_id AS STRING)           AS store_id,
  CAST(asds.product_id AS STRING)         AS product_id,
  DATE(asds.date)                         AS date,
  CAST(asds.total_sales_amount AS DOUBLE) AS total_sales_amount,
  CAST(asds.total_quantity_sold AS INT)   AS total_quantity_sold
FROM aggregated_sales_daily_silver asds
""")

# gold.gold_daily_refresh_log
gold_daily_refresh_log_df = spark.sql("""
SELECT
  DATE(drls.refresh_date)        AS refresh_date,
  CAST(drls.status AS STRING)    AS status,
  CAST(drls.row_count AS BIGINT) AS row_count
FROM daily_refresh_log_silver drls
""")

# -------------------------------------------------------------------
# 4) Save outputs (single CSV file per table directly under TARGET_PATH)
# -------------------------------------------------------------------
(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

(
    gold_daily_refresh_log_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_refresh_log.csv")
)

job.commit()