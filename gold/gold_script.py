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

# -----------------------------
# Read Source Tables from S3
# -----------------------------
ses_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)
sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_agg_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
ses_df.createOrReplaceTempView("ses")
sms_df.createOrReplaceTempView("sms")
pms_df.createOrReplaceTempView("pms")
dsas_df.createOrReplaceTempView("dsas")

# ============================================================
# Target Table: gold_sales
# ============================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ses.transaction_id AS STRING)     AS transaction_id,
        CAST(ses.product_id AS STRING)         AS product_id,
        CAST(ses.store_id AS STRING)           AS store_id,
        DATE(ses.sale_date)                    AS sale_date,
        CAST(ses.sale_amount AS DOUBLE)        AS sale_amount,
        CAST(ses.quantity_sold AS INT)         AS quantity_sold,
        CAST(ses.product_name AS STRING)       AS product_name,
        CAST(ses.product_category AS STRING)   AS product_category,
        CAST(ses.store_name AS STRING)         AS store_name,
        CAST(ses.store_location AS STRING)     AS store_location
    FROM ses
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target Table: gold_store_master
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)           AS store_id,
        CAST(sms.store_name AS STRING)         AS store_name,
        CAST(sms.store_location AS STRING)     AS store_location,
        CAST(sms.store_region AS STRING)       AS store_region
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

# ============================================================
# Target Table: gold_product_master
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)         AS product_id,
        CAST(pms.product_name AS STRING)       AS product_name,
        CAST(pms.product_category AS STRING)   AS product_category,
        CAST(pms.product_price AS FLOAT)       AS product_price
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

# ============================================================
# Target Table: gold_aggregated_sales
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(dsas.reporting_date)                  AS reporting_date,
        CAST(dsas.total_sales_amount AS DOUBLE)    AS total_sales_amount,
        CAST(dsas.total_quantity_sold AS INT)      AS total_quantity_sold,
        CAST(dsas.top_selling_product AS STRING)   AS top_selling_product,
        CAST(dsas.top_selling_store AS STRING)     AS top_selling_store
    FROM dsas
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()
