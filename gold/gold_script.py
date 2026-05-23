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

# ----------------------------
# Read source tables from S3
# ----------------------------
sts_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
ps_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
ss_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
sas_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ps_df.createOrReplaceTempView("product_silver")
ss_df.createOrReplaceTempView("store_silver")
sas_df.createOrReplaceTempView("sales_aggregated_silver")

# ============================
# Target: gold_sales
# ============================
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.sale_id AS STRING)            AS sale_id,
  CAST(sts.product_id AS STRING)         AS product_id,
  CAST(sts.store_id AS STRING)           AS store_id,
  DATE(sts.sale_date)                    AS sale_date,
  CAST(sts.quantity_sold AS INT)         AS quantity_sold,
  CAST(sts.total_sales_value AS DOUBLE)  AS total_sales_value
FROM sales_transactions_silver sts
""")

(
    gold_sales_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================
# Target: gold_product
# ============================
gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)          AS product_id,
  CAST(ps.product_name AS STRING)        AS product_name,
  CAST(ps.product_category AS STRING)    AS product_category,
  CAST(ps.product_price AS FLOAT)        AS product_price
FROM product_silver ps
""")

(
    gold_product_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ============================
# Target: gold_store
# ============================
gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)          AS store_id,
  CAST(ss.store_name AS STRING)        AS store_name,
  CAST(ss.store_location AS STRING)    AS store_location,
  CAST(ss.store_region AS STRING)      AS store_region
FROM store_silver ss
""")

(
    gold_store_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ============================
# Target: gold_sales_aggregated
# ============================
gold_sales_aggregated_df = spark.sql("""
SELECT
  DATE(sas.date)                          AS date,
  CAST(sas.total_sales AS DOUBLE)         AS total_sales,
  CAST(sas.total_quantity AS INT)         AS total_quantity,
  CAST(sas.average_sale_value AS DOUBLE)  AS average_sale_value,
  CAST(sas.region AS STRING)              AS region,
  CAST(sas.product_category AS STRING)    AS product_category
FROM sales_aggregated_silver sas
""")

(
    gold_sales_aggregated_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()