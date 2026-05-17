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
# Read source tables from S3 and create temp views
# ------------------------------------------------------------
product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
product_silver_df.createOrReplaceTempView("product_silver")

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
store_silver_df.createOrReplaceTempView("store_silver")

sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)
sales_silver_df.createOrReplaceTempView("sales_silver")

sales_aggregation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregation_silver.{FILE_FORMAT}/")
)
sales_aggregation_silver_df.createOrReplaceTempView("sales_aggregation_silver")

# ------------------------------------------------------------
# Target: gold_product
# ------------------------------------------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.brand AS STRING)        AS brand,
        CAST(ps.price AS FLOAT)         AS price
    FROM product_silver ps
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ------------------------------------------------------------
# Target: gold_store
# ------------------------------------------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)       AS store_id,
        CAST(ss.store_name AS STRING)     AS store_name,
        CAST(ss.location AS STRING)       AS location,
        CAST(ss.region AS STRING)         AS region,
        CAST(ss.store_manager AS STRING)  AS store_manager
    FROM store_silver ss
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ------------------------------------------------------------
# Target: gold_sales
# ------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sls.transaction_id AS STRING) AS transaction_id,
        DATE(sls.date)                     AS date,
        CAST(sls.product_id AS STRING)     AS product_id,
        CAST(sls.store_id AS STRING)       AS store_id,
        CAST(sls.quantity_sold AS INT)     AS quantity_sold,
        CAST(sls.sales_amount AS DOUBLE)   AS sales_amount
    FROM sales_silver sls
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ------------------------------------------------------------
# Target: gold_sales_aggregation
# ------------------------------------------------------------
gold_sales_aggregation_df = spark.sql(
    """
    SELECT
        DATE(sas.date)                           AS date,
        CAST(sas.store_id AS STRING)             AS store_id,
        CAST(sas.product_id AS STRING)           AS product_id,
        CAST(sas.total_quantity_sold AS INT)     AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE)   AS total_sales_amount,
        CAST(sas.average_price AS DOUBLE)        AS average_price
    FROM sales_aggregation_silver sas
    """
)

(
    gold_sales_aggregation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregation.csv")
)

job.commit()