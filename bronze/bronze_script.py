import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/src/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables from S3
# -----------------------------
products_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_raw.{FILE_FORMAT}/")
)

sales_transactions_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_raw.{FILE_FORMAT}/")
)

stores_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_raw.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
products_raw_df.createOrReplaceTempView("products_raw")
sales_transactions_raw_df.createOrReplaceTempView("sales_transactions_raw")
stores_raw_df.createOrReplaceTempView("stores_raw")

# -----------------------------
# Transform: products_raw_bronze
# -----------------------------
products_raw_bronze_df = spark.sql("""
SELECT
  prb.product_id AS product_id,
  prb.product_name AS product_name,
  prb.category AS category,
  prb.brand AS brand,
  prb.price AS price,
  prb.is_active AS is_active
FROM products_raw prb
""")

(
    products_raw_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_raw_bronze.csv")
)

# -----------------------------
# Transform: sales_transactions_raw_bronze
# -----------------------------
sales_transactions_raw_bronze_df = spark.sql("""
SELECT
  strb.transaction_id AS transaction_id,
  strb.store_id AS store_id,
  strb.product_id AS product_id,
  strb.quantity AS quantity,
  strb.sale_amount AS sale_amount,
  strb.transaction_time AS transaction_time
FROM sales_transactions_raw strb
""")

(
    sales_transactions_raw_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_raw_bronze.csv")
)

# -----------------------------
# Transform: stores_raw_bronze
# -----------------------------
stores_raw_bronze_df = spark.sql("""
SELECT
  srb.store_id AS store_id,
  srb.store_name AS store_name,
  srb.city AS city,
  srb.state AS state,
  srb.store_type AS store_type,
  srb.open_date AS open_date
FROM stores_raw srb
""")

(
    stores_raw_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_raw_bronze.csv")
)

job.commit()