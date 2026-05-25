import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------------------------
df_product_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

df_store_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

df_sales_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

df_sales_aggregated_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------------------------
df_product_silver.createOrReplaceTempView("product_silver")
df_store_silver.createOrReplaceTempView("store_silver")
df_sales_silver.createOrReplaceTempView("sales_silver")
df_sales_aggregated_silver.createOrReplaceTempView("sales_aggregated_silver")

# ------------------------------------------------------------------------------
# Target: gold_product
# ------------------------------------------------------------------------------
df_gold_product = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)      AS product_id,
        CAST(ps.product_name AS STRING)    AS product_name,
        CAST(ps.category AS STRING)        AS category,
        CAST(ps.price AS DECIMAL(38, 18))  AS price
    FROM product_silver ps
    """
)

df_gold_product.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_product.csv"
)

# ------------------------------------------------------------------------------
# Target: gold_store
# ------------------------------------------------------------------------------
df_gold_store = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)     AS store_id,
        CAST(ss.store_name AS STRING)   AS store_name,
        CAST(ss.location AS STRING)     AS location
    FROM store_silver ss
    """
)

df_gold_store.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_store.csv"
)

# ------------------------------------------------------------------------------
# Target: gold_sales
# ------------------------------------------------------------------------------
df_gold_sales = spark.sql(
    """
    SELECT
        CAST(sls.sale_id AS STRING)      AS sale_id,
        DATE(sls.date)                  AS date,
        CAST(sls.product_id AS STRING)   AS product_id,
        CAST(sls.store_id AS STRING)     AS store_id,
        CAST(sls.quantity AS INT)        AS quantity,
        CAST(sls.total_price AS DOUBLE)  AS total_price
    FROM sales_silver sls
    """
)

df_gold_sales.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_sales.csv"
)

# ------------------------------------------------------------------------------
# Target: gold_sales_aggregated
# ------------------------------------------------------------------------------
df_gold_sales_aggregated = spark.sql(
    """
    SELECT
        DATE(sas.date)                   AS date,
        CAST(sas.category AS STRING)     AS category,
        CAST(sas.region AS STRING)       AS region,
        CAST(sas.total_sales AS DOUBLE)  AS total_sales,
        CAST(sas.total_quantity AS INT)  AS total_quantity
    FROM sales_aggregated_silver sas
    """
)

df_gold_sales_aggregated.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_sales_aggregated.csv"
)

job.commit()