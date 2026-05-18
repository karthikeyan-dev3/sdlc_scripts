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

# ---------------------------------------------------------------------
# Read source tables from S3 (CSV)
# ---------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
drl_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_refresh_log_silver.{FILE_FORMAT}/")
)

# ---------------------------------------------------------------------
# Create temp views
# ---------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
sas_df.createOrReplaceTempView("sales_aggregated_silver")
drl_df.createOrReplaceTempView("data_refresh_log_silver")

# ---------------------------------------------------------------------
# Target: gold_sales_transactions
# ---------------------------------------------------------------------
gold_sales_transactions_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)       AS transaction_id,
  CAST(sts.store_id AS STRING)             AS store_id,
  CAST(sts.product_id AS STRING)           AS product_id,
  CAST(sts.quantity_sold AS INT)           AS quantity_sold,
  CAST(sts.total_sale_amount AS DOUBLE)    AS total_sale_amount,
  CAST(sts.sale_date AS DATE)              AS sale_date
FROM sales_transactions_silver sts
"""
gold_sales_transactions_df = spark.sql(gold_sales_transactions_sql)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ---------------------------------------------------------------------
# Target: gold_product_master
# ---------------------------------------------------------------------
gold_product_master_sql = """
SELECT
  CAST(pms.product_id AS STRING)           AS product_id,
  CAST(pms.product_name AS STRING)         AS product_name,
  CAST(pms.product_category AS STRING)     AS product_category,
  CAST(pms.price AS FLOAT)                 AS price
FROM product_master_silver pms
"""
gold_product_master_df = spark.sql(gold_product_master_sql)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ---------------------------------------------------------------------
# Target: gold_store_master
# ---------------------------------------------------------------------
gold_store_master_sql = """
SELECT
  CAST(sms.store_id AS STRING)             AS store_id,
  CAST(sms.store_name AS STRING)           AS store_name,
  CAST(sms.store_location AS STRING)       AS store_location,
  CAST(sms.store_type AS STRING)           AS store_type
FROM store_master_silver sms
"""
gold_store_master_df = spark.sql(gold_store_master_sql)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ---------------------------------------------------------------------
# Target: gold_sales_aggregated
# ---------------------------------------------------------------------
gold_sales_aggregated_sql = """
SELECT
  CAST(sas.aggregation_date AS DATE)           AS aggregation_date,
  CAST(sas.store_id AS STRING)                 AS store_id,
  CAST(sas.total_sales_amount AS DOUBLE)       AS total_sales_amount,
  CAST(sas.total_quantity_sold AS INT)         AS total_quantity_sold,
  CAST(sas.average_sale_per_transaction AS DOUBLE) AS average_sale_per_transaction
FROM sales_aggregated_silver sas
"""
gold_sales_aggregated_df = spark.sql(gold_sales_aggregated_sql)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ---------------------------------------------------------------------
# Target: gold_data_refresh_status
# ---------------------------------------------------------------------
gold_data_refresh_status_sql = """
SELECT
  CAST(drl.refresh_date AS DATE)              AS refresh_date,
  CAST(drl.data_source AS STRING)             AS data_source,
  CAST(drl.status AS STRING)                  AS status,
  CAST(drl.last_successful_refresh AS DATE)   AS last_successful_refresh
FROM data_refresh_log_silver drl
"""
gold_data_refresh_status_df = spark.sql(gold_data_refresh_status_sql)

(
    gold_data_refresh_status_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_refresh_status.csv")
)

job.commit()