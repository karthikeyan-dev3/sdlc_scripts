import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------
# Read Source Tables from S3
# -------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
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

sales_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")
)

data_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)

# -------------------------------------
# Create Temp Views
# -------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

# -------------------------------------
# gold.gold_sales (gs) from silver.sales_transactions_silver (sts)
# -------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)      AS transaction_id,
        DATE(sts.transaction_date)             AS transaction_date,
        CAST(sts.product_id AS STRING)         AS product_id,
        CAST(sts.store_id AS STRING)           AS store_id,
        CAST(sts.quantity_sold AS INT)         AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
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

# -------------------------------------
# gold.gold_product (gp) from silver.products_silver (ps)
# -------------------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.brand AS STRING)        AS brand,
        CAST(ps.price AS FLOAT)         AS price
    FROM products_silver ps
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# -------------------------------------
# gold.gold_store (gstore) from silver.stores_silver (ss)
# -------------------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)   AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING)   AS location,
        CAST(ss.region AS STRING)     AS region
    FROM stores_silver ss
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# -------------------------------------
# gold.gold_sales_aggregates (gsa) from silver.sales_aggregates_silver (sas)
# -------------------------------------
gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        DATE(sas.transaction_date)          AS transaction_date,
        CAST(sas.store_id AS STRING)        AS store_id,
        CAST(sas.product_id AS STRING)      AS product_id,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.total_revenue AS DOUBLE)   AS total_revenue
    FROM sales_aggregates_silver sas
    """
)

(
    gold_sales_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)

# -------------------------------------
# gold.gold_data_quality (gdq) from silver.data_quality_silver (dqs)
# -------------------------------------
gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqs.record_id AS STRING)          AS record_id,
        CAST(dqs.data_source AS STRING)        AS data_source,
        CAST(dqs.validation_status AS STRING)  AS validation_status,
        DATE(dqs.validation_date)              AS validation_date
    FROM data_quality_silver dqs
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()