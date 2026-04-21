
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Job init
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# CSV read options (adjust if your silver CSVs have different settings)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# -----------------------------------------------------------------------------------
# 1) Read source tables (S3) and create temp views
#    NOTE: Per requirement, ALWAYS use: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
sales_fact_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/sales_fact_silver.{FILE_FORMAT}/")
)
sales_fact_silver_df.createOrReplaceTempView("sales_fact_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

# ===================================================================================
# TARGET TABLE: gold_store_daily_sales (gsds)
# Mapping: silver.sales_fact_silver sfs INNER JOIN silver.stores_silver ss ON sfs.store_id = ss.store_id
# Transformations (EXACT from UDT):
#   gsds.sales_date = sfs.sales_date
#   gsds.store_id = ss.store_id
#   gsds.store_name = ss.store_name
#   gsds.store_type = ss.store_type
#   gsds.city = ss.city
#   gsds.state = ss.state
#   gsds.country = ss.country
#   gsds.total_revenue = SUM(sfs.revenue)
#   gsds.total_transactions = COUNT(DISTINCT sfs.transaction_id)
#   gsds.total_quantity_sold = SUM(sfs.quantity_sold)
# ===================================================================================
gold_store_daily_sales_sql = """
SELECT
  CAST(sfs.sales_date AS STRING)                                   AS sales_date,
  CAST(ss.store_id AS STRING)                                      AS store_id,
  CAST(ss.store_name AS STRING)                                    AS store_name,
  CAST(ss.store_type AS STRING)                                    AS store_type,
  CAST(ss.city AS STRING)                                          AS city,
  CAST(ss.state AS STRING)                                         AS state,
  CAST(ss.country AS STRING)                                       AS country,
  CAST(SUM(CAST(sfs.revenue AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(sfs.transaction_id AS STRING)) AS BIGINT) AS total_transactions,
  CAST(SUM(CAST(sfs.quantity_sold AS BIGINT)) AS BIGINT)           AS total_quantity_sold
FROM sales_fact_silver sfs
INNER JOIN stores_silver ss
  ON CAST(sfs.store_id AS STRING) = CAST(ss.store_id AS STRING)
GROUP BY
  CAST(sfs.sales_date AS STRING),
  CAST(ss.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.store_type AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING),
  CAST(ss.country AS STRING)
"""
gold_store_daily_sales_df = spark.sql(gold_store_daily_sales_sql)

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# ===================================================================================
# TARGET TABLE: gold_product_daily_sales (gpds)
# Mapping: silver.sales_fact_silver sfs INNER JOIN silver.products_silver ps ON sfs.product_id = ps.product_id
# Transformations (EXACT from UDT):
#   gpds.sales_date = sfs.sales_date
#   gpds.product_id = ps.product_id
#   gpds.product_name = ps.product_name
#   gpds.brand = ps.brand
#   gpds.category = ps.category
#   gpds.total_revenue = SUM(sfs.revenue)
#   gpds.total_quantity_sold = SUM(sfs.quantity_sold)
#   gpds.total_transactions = COUNT(DISTINCT sfs.transaction_id)
# ===================================================================================
gold_product_daily_sales_sql = """
SELECT
  CAST(sfs.sales_date AS STRING)                                   AS sales_date,
  CAST(ps.product_id AS STRING)                                    AS product_id,
  CAST(ps.product_name AS STRING)                                  AS product_name,
  CAST(ps.brand AS STRING)                                         AS brand,
  CAST(ps.category AS STRING)                                      AS category,
  CAST(SUM(CAST(sfs.revenue AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(SUM(CAST(sfs.quantity_sold AS BIGINT)) AS BIGINT)           AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(sfs.transaction_id AS STRING)) AS BIGINT) AS total_transactions
FROM sales_fact_silver sfs
INNER JOIN products_silver ps
  ON CAST(sfs.product_id AS STRING) = CAST(ps.product_id AS STRING)
GROUP BY
  CAST(sfs.sales_date AS STRING),
  CAST(ps.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.brand AS STRING),
  CAST(ps.category AS STRING)
"""
gold_product_daily_sales_df = spark.sql(gold_product_daily_sales_sql)

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

# ===================================================================================
# TARGET TABLE: gold_category_daily_sales (gcds)
# Mapping: silver.sales_fact_silver sfs INNER JOIN silver.products_silver ps ON sfs.product_id = ps.product_id
# Transformations (EXACT from UDT):
#   gcds.sales_date = sfs.sales_date
#   gcds.category = ps.category
#   gcds.total_revenue = SUM(sfs.revenue)
#   gcds.total_quantity_sold = SUM(sfs.quantity_sold)
#   gcds.total_transactions = COUNT(DISTINCT sfs.transaction_id)
# ===================================================================================
gold_category_daily_sales_sql = """
SELECT
  CAST(sfs.sales_date AS STRING)                                   AS sales_date,
  CAST(ps.category AS STRING)                                      AS category,
  CAST(SUM(CAST(sfs.revenue AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(SUM(CAST(sfs.quantity_sold AS BIGINT)) AS BIGINT)           AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(sfs.transaction_id AS STRING)) AS BIGINT) AS total_transactions
FROM sales_fact_silver sfs
INNER JOIN products_silver ps
  ON CAST(sfs.product_id AS STRING) = CAST(ps.product_id AS STRING)
GROUP BY
  CAST(sfs.sales_date AS STRING),
  CAST(ps.category AS STRING)
"""
gold_category_daily_sales_df = spark.sql(gold_category_daily_sales_sql)

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_category_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_category_daily_sales.csv")
)

job.commit()
