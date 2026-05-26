import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -------------------------
# Read Source Tables (S3)
# -------------------------
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
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
store_revenue_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_revenue_daily_silver.{FILE_FORMAT}/")
)
product_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_sales_daily_silver.{FILE_FORMAT}/")
)
category_performance_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/category_performance_daily_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create Temp Views
# -------------------------
product_details_silver_df.createOrReplaceTempView("product_details_silver")
store_details_silver_df.createOrReplaceTempView("store_details_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
store_revenue_daily_silver_df.createOrReplaceTempView("store_revenue_daily_silver")
product_sales_daily_silver_df.createOrReplaceTempView("product_sales_daily_silver")
category_performance_daily_silver_df.createOrReplaceTempView("category_performance_daily_silver")

# ============================================================
# Target: gold_product_master
# Mapping: silver.product_details_silver pds
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pds.product_id AS STRING)   AS product_id,
        CAST(pds.product_name AS STRING) AS product_name,
        CAST(pds.category AS STRING)     AS category,
        CAST(pds.brand AS STRING)        AS brand
    FROM product_details_silver pds
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
# Mapping: silver.store_details_silver sds
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sds.store_id AS STRING)     AS store_id,
        CAST(sds.store_name AS STRING)   AS store_name,
        CAST(sds.city AS STRING)         AS city,
        CAST(sds.state AS STRING)        AS state,
        CAST(sds.store_type AS STRING)   AS store_type
    FROM store_details_silver sds
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
# Target: gold_sales
# Mapping: silver.sales_transactions_silver sts
# ============================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING)           AS store_id,
        DATE(sts.transaction_date)            AS transaction_date,
        CAST(sts.product_id AS STRING)         AS product_id,
        CAST(sts.transaction_id AS STRING)     AS transaction_id,
        CAST(sts.quantity_sold AS INT)         AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE)      AS total_revenue
    FROM sales_transactions_silver sts
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
# Target: gold_store_performance
# Mapping: silver.store_revenue_daily_silver srds
#          INNER JOIN silver.store_details_silver sds ON srds.store_id = sds.store_id
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(srds.store_id AS STRING)              AS store_id,
        CAST(sds.store_name AS STRING)             AS store_name,
        CAST(sds.city AS STRING)                   AS city,
        CAST(sds.store_type AS STRING)             AS store_type,
        CAST(srds.total_revenue AS DOUBLE)         AS total_revenue,
        CAST(srds.total_transactions AS BIGINT)    AS total_transactions,
        DATE(srds.reporting_date)                  AS reporting_date
    FROM store_revenue_daily_silver srds
    INNER JOIN store_details_silver sds
        ON srds.store_id = sds.store_id
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold_product_performance
# Mapping: silver.product_sales_daily_silver psds
#          INNER JOIN silver.product_details_silver pds ON psds.product_id = pds.product_id
#          INNER JOIN silver.category_performance_daily_silver cpds
#            ON pds.category = cpds.category AND psds.transaction_date = cpds.transaction_date
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(psds.product_id AS STRING)        AS product_id,
        CAST(pds.product_name AS STRING)       AS product_name,
        CAST(pds.category AS STRING)           AS category,
        CAST(psds.quantity_sold AS BIGINT)     AS quantity_sold,
        CAST(psds.total_revenue AS DOUBLE) / CAST(cpds.category_revenue AS DOUBLE) AS revenue_contribution,
        CAST(cpds.category_revenue AS DOUBLE)  AS category_performance
    FROM product_sales_daily_silver psds
    INNER JOIN product_details_silver pds
        ON psds.product_id = pds.product_id
    INNER JOIN category_performance_daily_silver cpds
        ON pds.category = cpds.category
       AND psds.transaction_date = cpds.transaction_date
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)
