import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/src/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables (S3)
# ------------------------------------------------------------------------------
products_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_raw.{FILE_FORMAT}/")
)
products_raw_df.createOrReplaceTempView("products_raw")

sales_transactions_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_raw.{FILE_FORMAT}/")
)
sales_transactions_raw_df.createOrReplaceTempView("sales_transactions_raw")

stores_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_raw.{FILE_FORMAT}/")
)
stores_raw_df.createOrReplaceTempView("stores_raw")

# ------------------------------------------------------------------------------
# Target: bronze.products_bronze
# ------------------------------------------------------------------------------
products_bronze_df = spark.sql(
    """
    SELECT
        pr.product_id AS product_id,
        pr.product_name AS product_name,
        pr.category AS category,
        pr.brand AS brand,
        CAST(pr.price AS FLOAT) AS price,
        CAST(pr.is_active AS BOOLEAN) AS is_active
    FROM products_raw pr
    """
)

(
    products_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_bronze.csv")
)

# ------------------------------------------------------------------------------
# Target: bronze.sales_transactions_bronze
# ------------------------------------------------------------------------------
sales_transactions_bronze_df = spark.sql(
    """
    SELECT
        str.transaction_id AS transaction_id,
        str.store_id AS store_id,
        str.product_id AS product_id,
        CAST(str.quantity AS INT) AS quantity,
        CAST(str.sale_amount AS DOUBLE) AS sale_amount,
        CAST(str.transaction_time AS TIMESTAMP) AS transaction_time
    FROM sales_transactions_raw str
    """
)

(
    sales_transactions_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_bronze.csv")
)

# ------------------------------------------------------------------------------
# Target: bronze.stores_bronze
# ------------------------------------------------------------------------------
stores_bronze_df = spark.sql(
    """
    SELECT
        sr.store_id AS store_id,
        sr.store_name AS store_name,
        sr.city AS city,
        sr.state AS state,
        sr.store_type AS store_type,
        CAST(sr.open_date AS DATE) AS open_date
    FROM stores_raw sr
    """
)

(
    stores_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_bronze.csv")
)

job.commit()