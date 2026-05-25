import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------
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

ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
ass_df.createOrReplaceTempView("ass")

# ------------------------------------------------------------
# Target: gold.sales_transactions_gold
# ------------------------------------------------------------
sales_transactions_gold_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)      AS transaction_id,
        DATE(sts.transaction_date)             AS transaction_date,
        CAST(sts.store_id AS STRING)           AS store_id,
        CAST(sts.product_id AS STRING)         AS product_id,
        CAST(sts.quantity_sold AS INT)         AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE)       AS sales_amount,
        CAST(sts.discount_amount AS DOUBLE)    AS discount_amount
    FROM sts
    """
)

(
    sales_transactions_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_gold.csv")
)

# ------------------------------------------------------------
# Target: gold.product_master_gold
# ------------------------------------------------------------
product_master_gold_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)     AS product_id,
        CAST(pms.product_name AS STRING)   AS product_name,
        CAST(pms.category AS STRING)       AS category,
        CAST(pms.brand AS STRING)          AS brand,
        CAST(pms.unit_price AS DOUBLE)     AS unit_price
    FROM pms
    """
)

(
    product_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_gold.csv")
)

# ------------------------------------------------------------
# Target: gold.store_master_gold
# ------------------------------------------------------------
store_master_gold_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)      AS store_id,
        CAST(sms.store_name AS STRING)    AS store_name,
        CAST(sms.location AS STRING)      AS location,
        CAST(sms.region AS STRING)        AS region
    FROM sms
    """
)

(
    store_master_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_gold.csv")
)

# ------------------------------------------------------------
# Target: gold.aggregated_sales_gold
# ------------------------------------------------------------
aggregated_sales_gold_df = spark.sql(
    """
    SELECT
        DATE(ass.date)                             AS date,
        CAST(ass.store_id AS STRING)               AS store_id,
        CAST(ass.product_id AS STRING)             AS product_id,
        CAST(ass.total_quantity_sold AS INT)       AS total_quantity_sold,
        CAST(ass.total_sales_amount AS DOUBLE)     AS total_sales_amount,
        CAST(ass.total_discount_amount AS DOUBLE)  AS total_discount_amount,
        CAST(ass.average_unit_price AS DOUBLE)     AS average_unit_price
    FROM ass
    """
)

(
    aggregated_sales_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_gold.csv")
)

job.commit()