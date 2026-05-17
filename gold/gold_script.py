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

# -------------------------------------------------------------------
# Read Source Tables (Silver) + Create Temp Views
# -------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("silver_sales_transactions_silver")

pds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)
pds_df.createOrReplaceTempView("silver_product_details_silver")

sis_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")
)
sis_df.createOrReplaceTempView("silver_store_information_silver")

sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregation_silver.{FILE_FORMAT}/")
)
sas_df.createOrReplaceTempView("silver_sales_aggregation_silver")

# -------------------------------------------------------------------
# Target: gold.sales_transactions_gold
# -------------------------------------------------------------------
sales_transactions_gold_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)      AS transaction_id,
  DATE(sts.transaction_date)             AS transaction_date,
  CAST(sts.store_id AS STRING)           AS store_id,
  CAST(sts.product_id AS STRING)         AS product_id,
  CAST(sts.quantity_sold AS INT)         AS quantity_sold,
  CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM silver_sales_transactions_silver sts
""")

(
    sales_transactions_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_gold.csv")
)

# -------------------------------------------------------------------
# Target: gold.product_master_gold
# -------------------------------------------------------------------
product_master_gold_df = spark.sql("""
SELECT
  CAST(pds.product_id AS STRING)   AS product_id,
  CAST(pds.product_name AS STRING) AS product_name,
  CAST(pds.category AS STRING)     AS category,
  CAST(pds.brand AS STRING)        AS brand
FROM silver_product_details_silver pds
""")

(
    product_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_gold.csv")
)

# -------------------------------------------------------------------
# Target: gold.store_master_gold
# -------------------------------------------------------------------
store_master_gold_df = spark.sql("""
SELECT
  CAST(sis.store_id AS STRING)    AS store_id,
  CAST(sis.store_name AS STRING)  AS store_name,
  CAST(sis.location AS STRING)    AS location,
  CAST(sis.store_size AS STRING)  AS store_size
FROM silver_store_information_silver sis
""")

(
    store_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_gold.csv")
)

# -------------------------------------------------------------------
# Target: gold.sales_aggregated_gold
# -------------------------------------------------------------------
sales_aggregated_gold_df = spark.sql("""
SELECT
  DATE(sas.date)                         AS date,
  CAST(sas.store_id AS STRING)           AS store_id,
  CAST(sas.product_id AS STRING)         AS product_id,
  CAST(sas.total_quantity_sold AS INT)   AS total_quantity_sold,
  CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM silver_sales_aggregation_silver sas
""")

(
    sales_aggregated_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_gold.csv")
)

job.commit()