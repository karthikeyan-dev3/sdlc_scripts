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


# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
pms_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sms_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sts_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
sts_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL + 4) Write each target table separately
# ------------------------------------------------------------------------------

# --- gold_product_master ---
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)       AS product_id,
  CAST(pms.product_name AS STRING)     AS product_name,
  CAST(pms.product_category AS STRING) AS product_category,
  CAST(pms.product_price AS DOUBLE)    AS product_price
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

# --- gold_store_master ---
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)     AS store_id,
  CAST(sms.store_name AS STRING)   AS store_name,
  CAST(sms.store_region AS STRING) AS store_region,
  CAST(sms.store_type AS STRING)   AS store_type
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

# --- gold_sales_transactions ---
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)  AS transaction_id,
  DATE(sts.transaction_date)          AS transaction_date,
  CAST(sts.store_id AS STRING)        AS store_id,
  CAST(sts.product_id AS STRING)      AS product_id,
  CAST(sts.sale_amount AS DOUBLE)     AS sale_amount,
  CAST(sts.quantity_sold AS INT)      AS quantity_sold
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# --- gold_sales_aggregated ---
gold_sales_aggregated_df = spark.sql("""
SELECT
  DATE(sts.transaction_date)      AS aggregation_date,
  CAST(sts.store_id AS STRING)    AS store_id,
  CAST(sts.product_id AS STRING)  AS product_id,
  SUM(CAST(sts.sale_amount AS DOUBLE))    AS total_sales_amount,
  SUM(CAST(sts.quantity_sold AS INT))     AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  DATE(sts.transaction_date),
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING)
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