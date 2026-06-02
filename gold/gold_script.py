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

# -------------------------------------------------------------------
# Read Source Tables (S3)
# -------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create Temp Views
# -------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)       AS transaction_id,
  DATE(sts.transaction_date)              AS transaction_date,
  CAST(sts.store_id AS STRING)            AS store_id,
  CAST(sts.product_id AS STRING)          AS product_id,
  CAST(sts.quantity_sold AS INT)          AS quantity_sold,
  CAST(sts.total_sale_amount AS DOUBLE)   AS total_sale_amount,
  CAST(sts.customer_id AS STRING)         AS customer_id
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)     AS product_id,
  CAST(pms.product_name AS STRING)   AS product_name,
  CAST(pms.category AS STRING)       AS category,
  CAST(pms.price AS FLOAT)           AS price
FROM product_master_silver pms
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)      AS store_id,
  CAST(sms.store_name AS STRING)    AS store_name,
  CAST(sms.location AS STRING)      AS location,
  CAST(sms.region AS STRING)        AS region
FROM store_master_silver sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_sales_aggregation
# -------------------------------------------------------------------
gold_sales_aggregation_df = spark.sql("""
SELECT
  DATE(sts.transaction_date) AS aggregate_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(
    SUM(CAST(sts.total_sale_amount AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS DOUBLE))
    AS DOUBLE
  ) AS average_unit_price
FROM sales_transactions_silver sts
GROUP BY
  DATE(sts.transaction_date),
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING)
""")

(
    gold_sales_aggregation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregation.csv")
)

job.commit()