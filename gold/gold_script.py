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

# ============================================================
# Read Source Tables from S3
# ============================================================

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("product_silver")

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("store_silver")

sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")
)
sas_df.createOrReplaceTempView("sales_aggregates_silver")

mts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/metadata_tracking_silver.{FILE_FORMAT}/")
)
mts_df.createOrReplaceTempView("metadata_tracking_silver")

# ============================================================
# Target: gold_sales
# ============================================================

gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)     AS transaction_id,
        DATE(sts.transaction_date)            AS transaction_date,
        CAST(sts.product_id AS STRING)        AS product_id,
        CAST(sts.store_id AS STRING)          AS store_id,
        CAST(sts.quantity_sold AS INT)        AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    WHERE sts.valid_transaction_flag = 'true'
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target: gold_product
# ============================================================

gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)         AS product_id,
        CAST(ps.product_name AS STRING)       AS product_name,
        CAST(ps.category AS STRING)           AS category,
        CAST(ps.brand AS STRING)              AS brand,
        CAST(ps.price AS DOUBLE)              AS price
    FROM product_silver ps
    WHERE ps.is_active = 'true' OR ps.is_active IS NULL
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product.csv")
)

# ============================================================
# Target: gold_store
# ============================================================

gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)       AS store_id,
        CAST(ss.store_name AS STRING)     AS store_name,
        CAST(ss.location AS STRING)       AS location,
        CAST(ss.store_type AS STRING)     AS store_type
    FROM store_silver ss
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store.csv")
)

# ============================================================
# Target: gold_aggregated_sales
# ============================================================

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(sas.aggregation_date)            AS date,
        CAST(sas.product_id AS STRING)        AS product_id,
        CAST(sas.store_id AS STRING)          AS store_id,
        CAST(sas.total_quantity_sold AS INT)  AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_aggregates_silver sas
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ============================================================
# Target: gold_metadata
# ============================================================

gold_metadata_df = spark.sql(
    """
    SELECT
        DATE(mts.data_refresh_date)       AS refresh_date,
        CAST(mts.source_system AS STRING) AS data_source,
        CAST(mts.integration_status AS STRING) AS status
    FROM metadata_tracking_silver mts
    """
)

(
    gold_metadata_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_metadata.csv")
)

job.commit()
