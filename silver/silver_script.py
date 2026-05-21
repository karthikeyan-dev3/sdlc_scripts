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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# ------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# ------------------------------------------------------------------------------
# 3) Transform: products_silver (SQL)
# ------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name
        FROM products_bronze pb
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_name DESC
            ) AS rn
        FROM base
        WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
    )
    SELECT
        CAST(product_id AS STRING) AS product_id,
        CAST(product_name AS STRING) AS product_name
    FROM ranked
    WHERE rn = 1
    """
)

products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------------------------
# 4) Write: products_silver as SINGLE CSV directly under TARGET_PATH
# ------------------------------------------------------------------------------
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) Transform: stores_silver (SQL)
# ------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name
        FROM stores_bronze sb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY store_name DESC
            ) AS rn
        FROM base
        WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
    )
    SELECT
        CAST(store_id AS STRING) AS store_id,
        CAST(store_name AS STRING) AS store_name
    FROM ranked
    WHERE rn = 1
    """
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------------------------
# 4) Write: stores_silver as SINGLE CSV directly under TARGET_PATH
# ------------------------------------------------------------------------------
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) Transform: sales_transactions_silver (SQL)
# ------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(tb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(tb.store_id) AS STRING) AS store_id,
            CAST(TRIM(tb.product_id) AS STRING) AS product_id,
            CAST(tb.quantity AS INT) AS quantity,
            CAST(tb.sale_amount AS DOUBLE) AS sale_amount,
            CAST(tb.transaction_time AS TIMESTAMP) AS transaction_time
        FROM transactions_bronze tb
    ),
    joined AS (
        SELECT
            b.transaction_id,
            b.store_id,
            b.product_id,
            b.quantity,
            b.sale_amount,
            b.transaction_time
        FROM base b
        INNER JOIN stores_silver ss
            ON b.store_id = ss.store_id
        INNER JOIN products_silver ps
            ON b.product_id = ps.product_id
    ),
    ranked AS (
        SELECT
            transaction_id,
            store_id,
            product_id,
            quantity,
            sale_amount,
            transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
        WHERE transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
    )
    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(store_id AS STRING) AS store_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(quantity AS INT) AS quantity,
        CAST(sale_amount AS DOUBLE) AS sale_amount,
        CAST(transaction_time AS TIMESTAMP) AS transaction_time
    FROM ranked
    WHERE rn = 1
    """
)

# ------------------------------------------------------------------------------
# 4) Write: sales_transactions_silver as SINGLE CSV directly under TARGET_PATH
# ------------------------------------------------------------------------------
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()