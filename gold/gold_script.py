import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# -----------------------------
# Read source tables from S3
# -----------------------------
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)

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

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
transactions_silver_df.createOrReplaceTempView("transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

# ============================================================
# Target: gold_sales_transactions
# Source: silver.transactions_silver ts
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(ts.transaction_id AS STRING) AS transaction_id,
        DATE(ts.transaction_date)         AS transaction_date,
        CAST(ts.product_id AS STRING)     AS product_id,
        CAST(ts.store_id AS STRING)       AS store_id,
        CAST(ts.quantity_sold AS INT)     AS quantity_sold,
        CAST(ts.sales_amount AS DOUBLE)   AS sales_amount
    FROM transactions_silver ts
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
# Target: gold_product_attributes
# Source: silver.products_silver ps
# ============================================================
gold_product_attributes_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.brand AS STRING)        AS brand
    FROM products_silver ps
    """
)

(
    gold_product_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_attributes.csv")
)

# ============================================================
# Target: gold_store_attributes
# Source: silver.stores_silver ss
# ============================================================
gold_store_attributes_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)   AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.region AS STRING)     AS region
    FROM stores_silver ss
    """
)

(
    gold_store_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_attributes.csv")
)

# ============================================================
# Target: gold_sales_aggregated
# Source: silver.sales_aggregated_silver sas
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(sas.aggregation_date)             AS aggregation_date,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sas.total_quantity_sold AS INT)   AS total_quantity_sold,
        CAST(sas.region AS STRING)             AS region,
        CAST(sas.category AS STRING)           AS category
    FROM sales_aggregated_silver sas
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()