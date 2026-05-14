
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# --------------------------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# --------------------------------------------------------------------------------------

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# --------------------------------------------------------------------------------------
# Target: gold_product_master
# Mapping: silver.product_master_silver pms
# --------------------------------------------------------------------------------------

gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)     AS product_id,
  CAST(pms.product_name AS STRING)   AS product_name,
  CAST(pms.category AS STRING)       AS category,
  CAST(pms.brand AS STRING)          AS brand
FROM product_master_silver pms
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_store_master
# Mapping: silver.store_master_silver sms
# --------------------------------------------------------------------------------------

gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)     AS store_id,
  CAST(sms.store_name AS STRING)   AS store_name,
  CAST(sms.region AS STRING)       AS region,
  CAST(sms.store_type AS STRING)   AS store_type
FROM store_master_silver sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_sales_transactions
# Mapping: silver.sales_transactions_silver sts
# --------------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)             AS transaction_id,
  CAST(sts.product_id AS STRING)                 AS product_id,
  CAST(sts.store_id AS STRING)                   AS store_id,
  DATE(CAST(sts.transaction_date AS STRING))     AS transaction_date,
  CAST(sts.sales_amount AS DOUBLE)               AS sales_amount,
  CAST(sts.quantity_sold AS INT)                 AS quantity_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_sales_summary
# Mapping: silver.sales_transactions_silver sts
# --------------------------------------------------------------------------------------

gold_sales_summary_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING)                   AS store_id,
  CAST(sts.product_id AS STRING)                 AS product_id,
  DATE(CAST(sts.transaction_date AS STRING))     AS transaction_date,
  SUM(CAST(sts.sales_amount AS DOUBLE))          AS total_sales_amount,
  SUM(CAST(sts.quantity_sold AS INT))            AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING),
  DATE(CAST(sts.transaction_date AS STRING))
""")

(
    gold_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_summary.csv")
)

job.commit()