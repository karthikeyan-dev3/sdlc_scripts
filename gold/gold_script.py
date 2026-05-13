
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------
# Read Source Tables (Silver) + Temp Views
# -------------------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

spm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
spm_df.createOrReplaceTempView("product_master_silver")

ssm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
ssm_df.createOrReplaceTempView("store_master_silver")

sdsa_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregated_silver.{FILE_FORMAT}/")
)
sdsa_df.createOrReplaceTempView("daily_sales_aggregated_silver")

# -------------------------------------
# Target: gold.sales_transactions
# -------------------------------------

sales_transactions_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)      AS transaction_id,
  CAST(sts.store_id AS STRING)            AS store_id,
  CAST(sts.product_id AS STRING)          AS product_id,
  DATE(sts.transaction_date)              AS transaction_date,
  CAST(sts.quantity_sold AS INT)          AS quantity_sold,
  CAST(sts.total_sales_amount AS DOUBLE)  AS total_sales_amount
FROM sales_transactions_silver sts
"""

gsts_df = spark.sql(sales_transactions_sql)

(
    gsts_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions.csv")
)

# -------------------------------------
# Target: gold.product_master
# -------------------------------------

product_master_sql = """
SELECT
  CAST(spm.product_id AS STRING)     AS product_id,
  CAST(spm.product_name AS STRING)   AS product_name,
  CAST(spm.category AS STRING)       AS category,
  CAST(spm.price AS DOUBLE)          AS price
FROM product_master_silver spm
"""

gpm_df = spark.sql(product_master_sql)

(
    gpm_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master.csv")
)

# -------------------------------------
# Target: gold.store_master
# -------------------------------------

store_master_sql = """
SELECT
  CAST(ssm.store_id AS STRING)     AS store_id,
  CAST(ssm.store_name AS STRING)   AS store_name,
  CAST(ssm.region AS STRING)       AS region,
  DATE(ssm.opening_date)           AS opening_date
FROM store_master_silver ssm
"""

gsm_df = spark.sql(store_master_sql)

(
    gsm_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master.csv")
)

# -------------------------------------
# Target: gold.aggregated_sales
# -------------------------------------

aggregated_sales_sql = """
SELECT
  CAST(sdsa.store_id AS STRING)          AS store_id,
  CAST(sdsa.product_id AS STRING)        AS product_id,
  DATE(sdsa.transaction_date)            AS transaction_date,
  CAST(sdsa.total_revenue AS DOUBLE)     AS total_revenue,
  CAST(sdsa.units_sold AS INT)           AS units_sold
FROM daily_sales_aggregated_silver sdsa
"""

gas_df = spark.sql(aggregated_sales_sql)

(
    gas_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales.csv")
)

job.commit()
