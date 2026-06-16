import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =============================================================================
# Read Source Tables (S3) + Temp Views
# =============================================================================
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

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

# =============================================================================
# Target: gold_product
# =============================================================================
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)          AS product_id,
        CAST(ps.product_name AS STRING)        AS product_name,
        CAST(ps.product_category AS STRING)    AS product_category,
        CAST(ps.product_price AS DOUBLE)       AS product_price
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

# =============================================================================
# Target: gold_store
# =============================================================================
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)          AS store_id,
        CAST(ss.store_name AS STRING)        AS store_name,
        CAST(ss.store_location AS STRING)    AS store_location,
        CAST(ss.store_region AS STRING)      AS store_region
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

# =============================================================================
# Target: gold_sales
# =============================================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sls.transaction_id AS STRING)      AS transaction_id,
        CAST(sls.product_id AS STRING)          AS product_id,
        CAST(sls.store_id AS STRING)            AS store_id,
        CAST(sls.sale_date AS DATE)             AS sale_date,
        CAST(sls.quantity_sold AS INT)          AS quantity_sold,
        CAST(sls.total_sales_amount AS DOUBLE)  AS total_sales_amount
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

# =============================================================================
# Target: gold_sales_aggregated
# =============================================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sas.store_region AS STRING)             AS store_region,
        CAST(sas.product_category AS STRING)         AS product_category,
        CAST(sas.sale_date AS DATE)                  AS sale_date,
        CAST(sas.total_sales_amount AS DOUBLE)       AS total_sales_amount,
        CAST(sas.average_quantity_sold AS DOUBLE)    AS average_quantity_sold
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
