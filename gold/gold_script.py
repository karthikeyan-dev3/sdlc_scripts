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

# -------------------------------
# 1) Read source tables from S3
# -------------------------------
products_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

sales_summary_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_summary_silver.{FILE_FORMAT}/")
)

# -------------------------------
# 2) Create temp views
# -------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
sales_summary_silver_df.createOrReplaceTempView("sales_summary_silver")

# -------------------------------
# 3) Transform with Spark SQL
# -------------------------------

# gold_product_master (gpm) from silver.products_silver (ps)
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)         AS product_id,
  CAST(ps.product_name AS STRING)       AS product_name,
  CAST(ps.product_category AS STRING)   AS product_category,
  CAST(ps.brand AS STRING)              AS brand,
  CAST(ps.price AS DOUBLE)              AS price
FROM products_silver ps
""")

# gold_store_master (gsm) from silver.stores_silver (ss)
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)           AS store_id,
  CAST(ss.store_name AS STRING)         AS store_name,
  CAST(ss.store_location AS STRING)     AS store_location,
  CAST(ss.store_region AS STRING)       AS store_region
FROM stores_silver ss
""")

# gold_sales_transactions (gst) from silver.sales_transactions_silver (sts)
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)    AS transaction_id,
  DATE(sts.transaction_date)            AS transaction_date,
  CAST(sts.product_id AS STRING)        AS product_id,
  CAST(sts.store_id AS STRING)          AS store_id,
  CAST(sts.quantity_sold AS INT)        AS quantity_sold,
  CAST(sts.sales_amount AS DOUBLE)      AS sales_amount
FROM sales_transactions_silver sts
""")

# gold_sales_aggregated (gsa) from silver.sales_summary_silver (sss)
gold_sales_aggregated_df = spark.sql("""
SELECT
  DATE(sss.date)                        AS date,
  CAST(sss.store_id AS STRING)          AS store_id,
  CAST(sss.product_id AS STRING)        AS product_id,
  CAST(sss.total_quantity_sold AS INT)  AS total_quantity_sold,
  CAST(sss.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM sales_summary_silver sss
""")

# -------------------------------
# 4) Save outputs (single CSV file per table directly under TARGET_PATH)
# -------------------------------
(
    gold_product_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_sales_transactions_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

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