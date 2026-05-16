import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# Read Source Tables (S3)
# ----------------------------
sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_aggregates_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

# ============================================================
# Target: gold_sales_transactions
# Mapping: silver.sales_transactions_silver sts
#          LEFT JOIN silver.product_master_silver pms ON sts.product_id = pms.product_id
#          LEFT JOIN silver.store_master_silver sms ON sts.store_id = sms.store_id
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)           AS transaction_id,
        DATE(sts.transaction_date)                  AS transaction_date,
        CAST(sts.store_id AS STRING)                AS store_id,
        CAST(sts.product_id AS STRING)              AS product_id,
        CAST(sts.quantity_sold AS INT)              AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)      AS total_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    """
)

(
    gold_sales_transactions_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ============================================================
# Target: gold_product_master
# Mapping: silver.product_master_silver pms
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)      AS product_id,
        CAST(pms.product_name AS STRING)    AS product_name,
        CAST(pms.category AS STRING)        AS category,
        CAST(pms.price AS FLOAT)            AS price
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ============================================================
# Target: gold_store_master
# Mapping: silver.store_master_silver sms
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)        AS store_id,
        CAST(sms.store_name AS STRING)      AS store_name,
        CAST(sms.store_region AS STRING)    AS store_region
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ============================================================
# Target: gold_sales_aggregates
# Mapping: silver.sales_aggregates_silver sas
# ============================================================
gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        DATE(sas.date)                              AS date,
        CAST(sas.store_id AS STRING)                AS store_id,
        CAST(sas.product_id AS STRING)              AS product_id,
        CAST(sas.total_quantity_sold AS INT)        AS total_quantity_sold,
        CAST(sas.total_sales_revenue AS DOUBLE)     AS total_sales_revenue
    FROM sales_aggregates_silver sas
    """
)

(
    gold_sales_aggregates_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)