import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
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

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

cleaned_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/cleaned_sales_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")
cleaned_sales_silver_df.createOrReplaceTempView("cleaned_sales_silver")

# ------------------------------------------------------------
# 3) Transformations using Spark SQL
# 4) Save output (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------

# ---- gold_sales_transactions ----
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
  CAST(sts.units_sold AS INT) AS units_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ---- gold_product_master ----
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.brand AS STRING) AS brand
FROM products_silver ps
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ---- gold_store_master ----
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_location AS STRING) AS store_location
FROM stores_silver ss
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ---- gold_sales_aggregated ----
gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sas.store_id AS STRING) AS store_id,
  CAST(sas.product_id AS STRING) AS product_id,
  CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount,
  CAST(sas.total_units_sold AS INT) AS total_units_sold,
  CAST(sas.average_sales_amount AS DOUBLE) AS average_sales_amount,
  CAST(sas.record_date AS DATE) AS record_date
FROM sales_aggregated_silver sas
""")

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ---- gold_cleaned_sales ----
gold_cleaned_sales_df = spark.sql("""
SELECT
  CAST(css.transaction_id AS STRING) AS transaction_id,
  CAST(css.cleaned_product_id AS STRING) AS cleaned_product_id,
  CAST(css.cleaned_store_id AS STRING) AS cleaned_store_id,
  CAST(css.cleaned_transaction_date AS DATE) AS cleaned_transaction_date,
  CAST(css.cleaned_sales_amount AS DOUBLE) AS cleaned_sales_amount
FROM cleaned_sales_silver css
""")

(
    gold_cleaned_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_sales.csv")
)

job.commit()