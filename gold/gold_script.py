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

# ----------------------------
# Read Source Tables from S3
# ----------------------------
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

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ============================================================
# Target: gold_product_master
# Mapping: silver.product_master_silver pms
# ============================================================
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)  AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING)     AS category,
  CAST(pms.brand AS STRING)        AS brand,
  CAST(pms.price AS DOUBLE)        AS price
FROM product_master_silver pms
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ============================================================
# Target: gold_store_master
# Mapping: silver.store_master_silver sms
# ============================================================
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)        AS store_id,
  CAST(sms.store_location AS STRING)  AS store_location
FROM store_master_silver sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ============================================================
# Target: gold_sales
# Mapping:
# silver.sales_transactions_silver sts
# INNER JOIN silver.product_master_silver pms ON sts.product_id = pms.product_id
# INNER JOIN silver.store_master_silver sms ON sts.store_id = sms.store_id
# ============================================================
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  DATE(sts.sale_date)               AS sale_date,
  CAST(sts.product_id AS STRING)    AS product_id,
  CAST(sts.store_id AS STRING)      AS store_id,
  CAST(sts.quantity_sold AS INT)    AS quantity_sold,
  CAST(sts.total_revenue AS DOUBLE) AS total_revenue
FROM sales_transactions_silver sts
INNER JOIN product_master_silver pms
  ON sts.product_id = pms.product_id
INNER JOIN store_master_silver sms
  ON sts.store_id = sms.store_id
""")

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target: gold_aggregated_sales
# Mapping: silver.aggregated_sales_silver ass
# ============================================================
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(ass.store_id AS STRING)            AS store_id,
  CAST(ass.product_id AS STRING)          AS product_id,
  DATE(ass.aggregation_date)              AS aggregation_date,
  CAST(ass.total_quantity_sold AS INT)    AS total_quantity_sold,
  CAST(ass.total_sales_revenue AS DOUBLE) AS total_sales_revenue
FROM aggregated_sales_silver ass
""")

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()