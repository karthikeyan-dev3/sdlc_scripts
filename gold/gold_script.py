import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# Target: gold_product_master
# Mapping: silver.products_silver ps
# -----------------------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS price
    FROM products_silver ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_store_master
# Mapping: silver.stores_silver ss
# -----------------------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)    AS store_id,
        CAST(ss.store_name AS STRING)  AS store_name,
        CAST(ss.region AS STRING)      AS region,
        CAST(ss.store_type AS STRING)  AS store_type
    FROM stores_silver ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_sales
# Mapping: silver.sales_transactions_silver sts
# -----------------------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.quantity_sold AS INT)     AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE)   AS sales_amount
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_aggregated_sales
# Mapping: silver.sales_transactions_silver sts
# Transformations (provided in UDT columns):
#   aggregation_date = sts.transaction_date
#   total_revenue    = SUM(sts.sales_amount)
#   total_transactions = COUNT(DISTINCT sts.transaction_id)
# -----------------------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_date AS DATE)            AS aggregation_date,
        CAST(SUM(sts.sales_amount) AS DOUBLE)         AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT) AS total_transactions
    FROM sales_transactions_silver sts
    GROUP BY CAST(sts.transaction_date AS DATE)
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)