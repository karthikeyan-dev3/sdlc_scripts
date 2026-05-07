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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# READ SOURCE TABLES (S3)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# CREATE TEMP VIEWS
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# TABLE: transactions_silver
# ============================================================
transactions_silver_sql = """
WITH base AS (
    SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        pb.product_id AS product_id,
        DATE(stb.transaction_time) AS transaction_date,
        COALESCE(stb.quantity, 0) AS quantity_sold,
        COALESCE(stb.sale_amount, 0) AS total_revenue,
        CASE
            WHEN COALESCE(stb.quantity, 0) > 0 THEN COALESCE(stb.sale_amount, 0) / stb.quantity
            ELSE NULL
        END AS price_per_unit,
        stb.transaction_time AS transaction_time,
        ROW_NUMBER() OVER (
            PARTITION BY stb.transaction_id, stb.store_id, pb.product_id, stb.transaction_time
            ORDER BY stb.transaction_time DESC
        ) AS rn
    FROM sales_transactions_bronze stb
    LEFT JOIN products_bronze pb
        ON stb.product_id = pb.product_id
    WHERE stb.transaction_id IS NOT NULL
)
SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    total_revenue,
    price_per_unit
FROM base
WHERE rn = 1
"""

transactions_silver_df = spark.sql(transactions_silver_sql)

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/transactions_silver.csv")
)

# ============================================================
# TABLE: stores_silver
# ============================================================
stores_silver_sql = """
WITH base AS (
    SELECT
        sb.store_id AS store_id,
        ROW_NUMBER() OVER (
            PARTITION BY sb.store_id
            ORDER BY sb.store_id DESC
        ) AS rn
    FROM stores_bronze sb
    WHERE sb.store_id IS NOT NULL
)
SELECT
    store_id
FROM base
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# TABLE: products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
    SELECT
        pb.product_id AS product_id,
        ROW_NUMBER() OVER (
            PARTITION BY pb.product_id
            ORDER BY pb.product_id DESC
        ) AS rn
    FROM products_bronze pb
    WHERE pb.product_id IS NOT NULL
)
SELECT
    product_id
FROM base
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

job.commit()