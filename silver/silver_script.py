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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables (Bronze) from S3
# ------------------------------------------------------------------------------

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ------------------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.product_id) AS STRING)     AS product_id,
            CAST(TRIM(stb.store_id) AS STRING)       AS store_id,
            CAST(stb.transaction_time AS DATE)       AS sale_date,
            COALESCE(CAST(stb.quantity AS INT), 0)   AS quantity_sold,
            COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) AS total_sale_amount,
            stb.transaction_time                     AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            quantity_sold,
            total_sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sale_date,
        quantity_sold,
        total_sale_amount
    FROM dedup
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.product_silver
# ------------------------------------------------------------------------------

product_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING)  AS product_id,
            pb.product_name                      AS product_name,
            pb.category                          AS category,
            pb.brand                             AS brand,
            CAST(pb.price AS FLOAT)              AS price
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN brand IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.store_silver
# ------------------------------------------------------------------------------

store_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            sb.store_name                     AS store_name,
            CONCAT_WS(', ', sb.city, sb.state) AS location,
            sb.store_type                     AS store_type,
            sb.city                           AS city,
            sb.state                          AS state
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN state IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_type IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        store_type
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

job.commit()