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

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# Target: gold.gold_sales_transactions
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)       AS transaction_id,
        DATE(sts.transaction_date)              AS transaction_date,
        CAST(sts.store_id AS STRING)            AS store_id,
        CAST(sts.product_id AS STRING)          AS product_id,
        CAST(sts.quantity_sold AS INT)          AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)  AS total_sales_amount
    FROM sales_transactions_silver sts
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
# Target: gold.gold_product_master
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)    AS product_id,
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

# ============================================================
# Target: gold.gold_store_master
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)    AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING)   AS location,
        CAST(sms.region AS STRING)     AS region,
        CAST(sms.store_type AS STRING) AS store_type
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

# ============================================================
# Target: gold.gold_sales_aggregated_daily
# ============================================================
gold_sales_aggregated_daily_df = spark.sql(
    """
    SELECT
        DATE(sts.transaction_date) AS date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_units_sold,
        CAST(
            SUM(CAST(sts.total_sales_amount AS DOUBLE)) / COUNT(DISTINCT sts.transaction_id)
            AS DOUBLE
        ) AS average_transaction_value
    FROM sales_transactions_silver sts
    GROUP BY
        DATE(sts.transaction_date),
        CAST(sts.store_id AS STRING)
    """
)

(
    gold_sales_aggregated_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_daily.csv")
)

# ============================================================
# Target: gold.gold_sales_aggregated_product
# ============================================================
gold_sales_aggregated_product_df = spark.sql(
    """
    SELECT
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_units_sold,
        CAST(
            SUM(CAST(sts.total_sales_amount AS DOUBLE)) / COUNT(DISTINCT DATE(sts.transaction_date))
            AS DOUBLE
        ) AS daily_average_sales
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.product_id AS STRING)
    """
)

(
    gold_sales_aggregated_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated_product.csv")
)