import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
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
ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
ass_df.createOrReplaceTempView("aggregated_sales_silver")

# -------------------------------------------------------------------
# 3) Transformations using Spark SQL (per target table)
# -------------------------------------------------------------------

# sales_transactions_gold
sales_transactions_gold_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)                    AS transaction_id,
  DATE(CAST(sts.date AS STRING))                        AS date,
  CAST(sts.product_id AS STRING)                        AS product_id,
  CAST(sts.store_id AS STRING)                          AS store_id,
  CAST(sts.quantity_sold AS INT)                        AS quantity_sold,
  CAST(sts.sales_amount AS DECIMAL(18,2))               AS sales_amount,
  CAST(sts.transaction_type AS STRING)                  AS transaction_type
FROM sales_transactions_silver sts
""")

# product_master_gold
product_master_gold_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING)                        AS product_id,
  CAST(pms.product_name AS STRING)                      AS product_name,
  CAST(pms.category AS STRING)                          AS category,
  CAST(pms.brand AS STRING)                             AS brand,
  CAST(pms.price AS DECIMAL(18,2))                      AS price,
  CAST(pms.status AS STRING)                            AS status
FROM product_master_silver pms
""")

# store_master_gold
store_master_gold_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING)                          AS store_id,
  CAST(sms.store_name AS STRING)                        AS store_name,
  CAST(sms.location AS STRING)                          AS location,
  CAST(sms.region AS STRING)                            AS region,
  DATE(CAST(sms.open_date AS STRING))                   AS open_date,
  CAST(sms.status AS STRING)                            AS status
FROM store_master_silver sms
""")

# aggregated_sales_gold
aggregated_sales_gold_df = spark.sql("""
SELECT
  CAST(ass.region AS STRING)                            AS region,
  CAST(ass.store_id AS STRING)                          AS store_id,
  CAST(ass.product_id AS STRING)                        AS product_id,
  CAST(ass.category AS STRING)                          AS category,
  DATE(CAST(ass.date AS STRING))                        AS date,
  CAST(ass.total_sales_amount AS DECIMAL(18,2))         AS total_sales_amount,
  CAST(ass.total_quantity_sold AS INT)                  AS total_quantity_sold
FROM aggregated_sales_silver ass
""")

# -------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# -------------------------------------------------------------------
(
    sales_transactions_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_gold.csv")
)

(
    product_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_gold.csv")
)

(
    store_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_gold.csv")
)

(
    aggregated_sales_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_gold.csv")
)

job.commit()