import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ---------------------------
# Read Sources + Temp Views
# ---------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sts")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("pms")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("sms")

dss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_summary_silver.{FILE_FORMAT}/")
)
dss_df.createOrReplaceTempView("dss")

# ---------------------------
# Target: sales_transactions
# ---------------------------

sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.total_amount AS DOUBLE) AS total_amount
    FROM sts
    """
)

(
    sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions.csv")
)

# ---------------------------
# Target: product_master
# ---------------------------

product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.price AS FLOAT) AS price,
        CAST(pms.brand AS STRING) AS brand
    FROM pms
    """
)

(
    product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master.csv")
)

# ---------------------------
# Target: store_master
# ---------------------------

store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.store_type AS STRING) AS store_type,
        CAST(sms.opening_date AS DATE) AS opening_date
    FROM sms
    """
)

(
    store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master.csv")
)

# ---------------------------
# Target: aggregated_sales
# ---------------------------

aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(dss.date AS DATE) AS date,
        CAST(dss.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(dss.total_units_sold AS BIGINT) AS total_units_sold,
        CAST(dss.average_transaction_value AS DOUBLE) AS average_transaction_value,
        CAST(dss.top_selling_product_id AS STRING) AS top_selling_product_id,
        CAST(dss.top_selling_store_id AS STRING) AS top_selling_store_id
    FROM dss
    """
)

(
    aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales.csv")
)

job.commit()