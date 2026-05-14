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

# =============================================================================
# 1) Read source tables
# =============================================================================
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

# =============================================================================
# 2) Create temp views
# =============================================================================
sts_df.createOrReplaceTempView("sts")
pms_df.createOrReplaceTempView("pms")
sms_df.createOrReplaceTempView("sms")
sas_df.createOrReplaceTempView("sas")

# =============================================================================
# 3) Transform + 4) Write each target table separately
# =============================================================================

# -----------------------------------------------------------------------------
# Target: gold_sales_transactions
# -----------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)   AS transaction_id,
        DATE(sts.sale_date)                 AS sale_date,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(sts.quantity_sold AS INT)      AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -----------------------------------------------------------------------------
# Target: gold_product_master
# -----------------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)      AS product_id,
        CAST(pms.product_name AS STRING)    AS product_name,
        CAST(pms.category AS STRING)        AS category,
        CAST(pms.brand AS STRING)           AS brand,
        CAST(pms.price AS FLOAT)            AS price
    FROM pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------------------------------------------------------
# Target: gold_store_master
# -----------------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)     AS store_id,
        CAST(sms.store_name AS STRING)  AS store_name,
        CAST(sms.location AS STRING)    AS location,
        CAST(sms.region AS STRING)      AS region
    FROM sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------------------------------------------------------
# Target: gold_sales_aggregated
# -----------------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(sas.sale_date)                 AS sale_date,
        CAST(sas.product_id AS STRING)      AS product_id,
        CAST(sas.store_id AS STRING)        AS store_id,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sas
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# -----------------------------------------------------------------------------
# Target: gold_sales_cleaned
# Note: UDT does not specify dedup keys/order; pass-through mapping only.
# -----------------------------------------------------------------------------
gold_sales_cleaned_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)   AS transaction_id,
        DATE(sts.sale_date)                 AS sale_date,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(sts.quantity_sold AS INT)      AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sts
    """
)

(
    gold_sales_cleaned_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_cleaned.csv")
)

job.commit()