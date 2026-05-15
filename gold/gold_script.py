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

# ------------------------------------------------------------
# Read source tables from S3 (CSV)
# ------------------------------------------------------------
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

stcs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_clean_silver.{FILE_FORMAT}/")
)

ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

kms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/kpi_metrics_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
stcs_df.createOrReplaceTempView("sales_transactions_clean_silver")
ass_df.createOrReplaceTempView("aggregated_sales_silver")
kms_df.createOrReplaceTempView("kpi_metrics_silver")

# ------------------------------------------------------------
# Target: gold_product_master
# Source: silver.product_master_silver pms
# ------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)   AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING)     AS category,
        CAST(pms.brand AS STRING)        AS brand
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------
# Target: gold_store_master
# Source: silver.store_master_silver sms
# ------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)        AS store_id,
        CAST(sms.store_location AS STRING)  AS store_location,
        CAST(sms.store_size AS STRING)      AS store_size
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------
# Target: gold_cleaned_data
# Source: silver.sales_transactions_clean_silver stcs
# ------------------------------------------------------------
gold_cleaned_data_df = spark.sql(
    """
    SELECT
        CAST(stcs.transaction_id AS STRING) AS transaction_id,
        DATE(stcs.sale_date)               AS sale_date,
        CAST(stcs.product_id AS STRING)    AS product_id,
        CAST(stcs.store_id AS STRING)      AS store_id,
        CAST(stcs.quantity_sold AS INT)    AS quantity_sold,
        CAST(stcs.total_amount AS DOUBLE)  AS total_amount
    FROM sales_transactions_clean_silver stcs
    """
)

(
    gold_cleaned_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_data.csv")
)

# ------------------------------------------------------------
# Target: gold_sales_transactions
# Source: silver.sales_transactions_clean_silver stcs
# ------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(stcs.transaction_id AS STRING) AS transaction_id,
        DATE(stcs.sale_date)               AS sale_date,
        CAST(stcs.product_id AS STRING)    AS product_id,
        CAST(stcs.store_id AS STRING)      AS store_id,
        CAST(stcs.quantity_sold AS INT)    AS quantity_sold,
        CAST(stcs.total_amount AS DOUBLE)  AS total_amount
    FROM sales_transactions_clean_silver stcs
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------
# Target: gold_aggregated_sales
# Source: silver.aggregated_sales_silver ass
# ------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING)              AS store_id,
        CAST(ass.product_id AS STRING)            AS product_id,
        CAST(ass.total_sales AS DOUBLE)           AS total_sales,
        CAST(ass.average_sale_price AS DOUBLE)    AS average_sale_price,
        CAST(ass.total_quantity_sold AS BIGINT)   AS total_quantity_sold
    FROM aggregated_sales_silver ass
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ------------------------------------------------------------
# Target: gold_kpi_metrics
# Source: silver.kpi_metrics_silver kms
# ------------------------------------------------------------
gold_kpi_metrics_df = spark.sql(
    """
    SELECT
        CAST(kms.metric_name AS STRING)  AS metric_name,
        CAST(kms.metric_value AS DOUBLE) AS metric_value,
        CAST(kms.target_value AS DOUBLE) AS target_value
    FROM kpi_metrics_silver kms
    """
)

(
    gold_kpi_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_kpi_metrics.csv")
)

job.commit()