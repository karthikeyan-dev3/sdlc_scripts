import sys
from awsglue.transforms import *
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

# ============================================================
# Read Source Tables (S3) + Temp Views
# ============================================================

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_aggregation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregation_silver.{FILE_FORMAT}/")
)
sales_aggregation_silver_df.createOrReplaceTempView("sales_aggregation_silver")

sales_validation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_validation_silver.{FILE_FORMAT}/")
)
sales_validation_silver_df.createOrReplaceTempView("sales_validation_silver")

# ============================================================
# Target: gold.gold_sales_transactions
# Source: silver.sales_transactions_silver sts
# ============================================================

gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)        AS transaction_id,
        DATE(sts.transaction_date)               AS transaction_date,
        CAST(sts.store_id AS STRING)             AS store_id,
        CAST(sts.product_id AS STRING)           AS product_id,
        CAST(sts.quantity_sold AS INT)           AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)   AS total_sales_amount
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
# Source: silver.products_silver ps
# ============================================================

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)          AS product_id,
        CAST(ps.product_name AS STRING)        AS product_name,
        CAST(ps.product_category AS STRING)    AS product_category,
        CAST(ps.product_price AS FLOAT)        AS product_price
    FROM products_silver ps
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
# Source: silver.stores_silver ss
# ============================================================

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)           AS store_id,
        CAST(ss.store_location AS STRING)     AS store_location,
        CAST(ss.store_type AS STRING)         AS store_type
    FROM stores_silver ss
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
# Target: gold.gold_sales_aggregates
# Source: silver.sales_aggregation_silver sas
# ============================================================

gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        DATE(sas.date)                        AS date,
        CAST(sas.store_id AS STRING)          AS store_id,
        CAST(sas.product_category AS STRING)  AS product_category,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sas.total_quantity_sold AS INT)  AS total_quantity_sold
    FROM sales_aggregation_silver sas
    """
)

(
    gold_sales_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)

# ============================================================
# Target: gold.gold_sales_cleaned
# Source: silver.sales_validation_silver svs
#         INNER JOIN silver.sales_transactions_silver sts
#         ON svs.transaction_id = sts.transaction_id
# ============================================================

gold_sales_cleaned_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)      AS transaction_id,
        CAST(sts.store_id AS STRING)            AS store_id,
        CAST(sts.product_id AS STRING)          AS product_id,
        CAST(svs.validated_transaction AS BOOLEAN) AS validated_transaction,
        CAST(svs.clean_history AS STRING)       AS clean_history
    FROM sales_validation_silver svs
    INNER JOIN sales_transactions_silver sts
        ON svs.transaction_id = sts.transaction_id
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