import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# Read source tables from S3
# ----------------------------
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
dqms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
sts_df.createOrReplaceTempView("sts")
pms_df.createOrReplaceTempView("pms")
sms_df.createOrReplaceTempView("sms")
sas_df.createOrReplaceTempView("sas")
dqms_df.createOrReplaceTempView("dqms")

# ============================================================
# Target: gold_sales_transactions
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.date AS DATE) AS date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.total_sales AS DOUBLE) AS total_sales
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

# ============================================================
# Target: gold_product_master
# ============================================================
gold_product_master_df = spark.sql(
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
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ============================================================
# Target: gold_store_master
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.region AS STRING) AS region
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
# Target: gold_sales_aggregated
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sas.date AS DATE) AS date,
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.product_id AS STRING) AS product_id,
        CAST(sas.total_quantity AS BIGINT) AS total_quantity,
        CAST(sas.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sas.category_revenue AS DOUBLE) AS category_revenue
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

# ============================================================
# Target: gold_data_quality_metrics
# ============================================================
gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        CAST(dqms.date AS DATE) AS date,
        CAST(dqms.total_records AS BIGINT) AS total_records,
        CAST(dqms.duplicate_records AS BIGINT) AS duplicate_records,
        CAST(dqms.invalid_records AS BIGINT) AS invalid_records,
        CAST(dqms.accuracy_score AS DOUBLE) AS accuracy_score
    FROM dqms
    """
)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()