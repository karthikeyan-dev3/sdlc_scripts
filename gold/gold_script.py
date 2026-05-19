import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------------------------
sales_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_details_silver.{FILE_FORMAT}/")
)
product_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)
store_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")
)
sales_aggregations_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregations_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
sales_details_silver_df.createOrReplaceTempView("sales_details_silver")
product_details_silver_df.createOrReplaceTempView("product_details_silver")
store_details_silver_df.createOrReplaceTempView("store_details_silver")
sales_aggregations_silver_df.createOrReplaceTempView("sales_aggregations_silver")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save outputs (one file per table directly under TARGET_PATH)
# --------------------------------------------------------------------------------------

# gold_sales_transactions (gst) from silver.sales_details_silver (sls)
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sls.transaction_id AS STRING)                AS transaction_id,
  CAST(sls.store_id AS STRING)                      AS store_id,
  CAST(sls.product_id AS STRING)                    AS product_id,
  CAST(sls.transaction_date AS DATE)                AS transaction_date,
  CAST(sls.quantity_sold AS INT)                    AS quantity_sold,
  CAST(sls.total_revenue AS DECIMAL(14,2))          AS total_revenue
FROM sales_details_silver sls
""")

(
    gold_sales_transactions_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# gold_product_master (gpm) from silver.product_details_silver (pds)
gold_product_master_df = spark.sql("""
SELECT
  CAST(pds.product_id AS STRING)                    AS product_id,
  CAST(pds.product_name AS STRING)                  AS product_name,
  CAST(pds.category AS STRING)                      AS category,
  CAST(pds.brand AS STRING)                         AS brand,
  CAST(pds.price AS DECIMAL(12,2))                  AS price
FROM product_details_silver pds
""")

(
    gold_product_master_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# gold_store_master (gsm) from silver.store_details_silver (sds)
gold_store_master_df = spark.sql("""
SELECT
  CAST(sds.store_id AS STRING)                      AS store_id,
  CAST(sds.store_name AS STRING)                    AS store_name,
  CAST(sds.location AS STRING)                      AS location,
  CAST(sds.store_type AS STRING)                    AS store_type
FROM store_details_silver sds
""")

(
    gold_store_master_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# gold_sales_aggregated (gsa) from silver.sales_aggregations_silver (sas)
gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sas.store_id AS STRING)                      AS store_id,
  CAST(sas.product_id AS STRING)                    AS product_id,
  CAST(sas.reporting_date AS DATE)                  AS reporting_date,
  CAST(sas.total_quantity_sold AS BIGINT)           AS total_quantity_sold,
  CAST(sas.total_store_revenue AS DECIMAL(14,2))    AS total_store_revenue,
  CAST(sas.top_selling_product AS STRING)           AS top_selling_product
FROM sales_aggregations_silver sas
""")

(
    gold_sales_aggregated_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()