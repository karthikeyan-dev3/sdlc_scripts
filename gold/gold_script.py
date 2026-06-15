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

# -----------------------------
# Read Source Tables from S3
# -----------------------------
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
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
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

# -----------------------------
# Create Temp Views
# -----------------------------
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
sts_df.createOrReplaceTempView("sales_transactions_silver")
sas_df.createOrReplaceTempView("sales_aggregated_silver")
dqms_df.createOrReplaceTempView("data_quality_metrics_silver")

# ============================================================
# Target: gold.product_master
# Source: silver.product_master_silver pms
# ============================================================
product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)     AS product_id,
        CAST(pms.product_name AS STRING)   AS product_name,
        CAST(pms.category AS STRING)       AS category,
        CAST(pms.price AS FLOAT)           AS price,
        CAST(pms.brand AS STRING)          AS brand
    FROM product_master_silver pms
    """
)

(
    product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master.csv")
)

# ============================================================
# Target: gold.store_master
# Source: silver.store_master_silver sms
# ============================================================
store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)     AS store_id,
        CAST(sms.store_name AS STRING)   AS store_name,
        CAST(sms.location AS STRING)     AS location,
        CAST(sms.region AS STRING)       AS region
    FROM store_master_silver sms
    """
)

(
    store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master.csv")
)

# ============================================================
# Target: gold.sales_transactions
# Source: silver.sales_transactions_silver sts
#         INNER JOIN silver.product_master_silver pms ON pms.product_id = sts.product_id
#         INNER JOIN silver.store_master_silver sms ON sms.store_id = sts.store_id
# ============================================================
sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)  AS transaction_id,
        DATE(sts.date)                      AS date,
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.quantity AS INT)           AS quantity,
        CAST(sts.total_sales AS DOUBLE)     AS total_sales
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON CAST(pms.product_id AS STRING) = CAST(sts.product_id AS STRING)
    INNER JOIN store_master_silver sms
        ON CAST(sms.store_id AS STRING) = CAST(sts.store_id AS STRING)
    """
)

(
    sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions.csv")
)

# ============================================================
# Target: gold.sales_aggregated
# Source: silver.sales_aggregated_silver sas
#         INNER JOIN silver.product_master_silver pms ON pms.product_id = sas.product_id
#         INNER JOIN silver.store_master_silver sms ON sms.store_id = sas.store_id
# ============================================================
sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(sas.date)                        AS date,
        CAST(sas.store_id AS STRING)          AS store_id,
        CAST(sas.product_id AS STRING)        AS product_id,
        CAST(sas.total_quantity AS BIGINT)    AS total_quantity,
        CAST(sas.total_revenue AS DOUBLE)     AS total_revenue,
        CAST(sas.category_revenue AS DOUBLE)  AS category_revenue
    FROM sales_aggregated_silver sas
    INNER JOIN product_master_silver pms
        ON CAST(pms.product_id AS STRING) = CAST(sas.product_id AS STRING)
    INNER JOIN store_master_silver sms
        ON CAST(sms.store_id AS STRING) = CAST(sas.store_id AS STRING)
    """
)

(
    sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated.csv")
)

# ============================================================
# Target: gold.data_quality_metrics
# Source: silver.data_quality_metrics_silver dqms
# ============================================================
data_quality_metrics_df = spark.sql(
    """
    SELECT
        DATE(dqms.date)                         AS date,
        CAST(dqms.total_records AS BIGINT)      AS total_records,
        CAST(dqms.duplicate_records AS BIGINT)  AS duplicate_records,
        CAST(dqms.invalid_records AS BIGINT)    AS invalid_records,
        CAST(dqms.accuracy_score AS DOUBLE)     AS accuracy_score
    FROM data_quality_metrics_silver dqms
    """
)

(
    data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics.csv")
)

job.commit()