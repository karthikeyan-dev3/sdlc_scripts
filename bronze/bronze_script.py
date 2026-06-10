import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/src/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ============================================================
# Read Source Tables (S3) + Create Temp Views
# ============================================================

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

# ============================================================
# Target: bronze.products_bronze
# Source: products_raw pr
# ============================================================

products_bronze_df = spark.sql(
    """
    SELECT
        CAST(pr.product_id AS STRING)   AS product_id,
        CAST(pr.product_name AS STRING) AS product_name,
        CAST(pr.category AS STRING)     AS category,
        CAST(pr.brand AS STRING)        AS brand,
        CAST(pr.price AS DOUBLE)        AS price,
        CAST(pr.is_active AS BOOLEAN)   AS is_active
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

# ============================================================
# Target: bronze.sales_transactions_bronze
# Source: sales_transactions_raw str
# ============================================================

sales_transactions_bronze_df = spark.sql(
    """
    SELECT
        CAST(str.transaction_id AS STRING)   AS transaction_id,
        CAST(str.store_id AS STRING)         AS store_id,
        CAST(str.product_id AS STRING)       AS product_id,
        CAST(str.quantity AS INT)            AS quantity,
        CAST(str.sale_amount AS DOUBLE)      AS sale_amount,
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

# ============================================================
# Target: bronze.stores_bronze
# Source: stores_raw sr
# ============================================================

stores_bronze_df = spark.sql(
    """
    SELECT
        CAST(sr.store_id AS STRING)     AS store_id,
        CAST(sr.store_name AS STRING)   AS store_name,
        CAST(sr.city AS STRING)         AS city,
        CAST(sr.state AS STRING)        AS state,
        CAST(sr.store_type AS STRING)   AS store_type,
        DATE(CAST(sr.open_date AS DATE)) AS open_date
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
