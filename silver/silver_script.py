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

# ------------------------------------------------------------
# Read source tables (Bronze)
# ------------------------------------------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# Target: silver.product_silver
# ------------------------------------------------------------
product_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            TRIM(pb.brand) AS brand,
            CAST(pb.price AS FLOAT) AS price,
            pb.is_active AS is_active
        FROM products_bronze pb
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
                    CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
        WHERE
            product_id IS NOT NULL
            AND TRIM(product_id) <> ''
            AND COALESCE(LOWER(TRIM(is_active)), 'false') = 'true'
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
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.store_silver
# ------------------------------------------------------------
store_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            CAST(sb.open_date AS DATE) AS opening_date
        FROM stores_bronze sb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            opening_date,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN location IS NOT NULL AND TRIM(location) <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN opening_date IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
        WHERE
            store_id IS NOT NULL
            AND TRIM(store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        location,
        opening_date
    FROM ranked
    WHERE rn = 1
    """
)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.sales_silver
# ------------------------------------------------------------
sales_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS date,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_value,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        LEFT JOIN product_silver ps
            ON TRIM(stb.product_id) = ps.product_id
        LEFT JOIN store_silver ss
            ON TRIM(stb.store_id) = ss.store_id
    ),
    filtered AS (
        SELECT
            transaction_id,
            date,
            product_id,
            store_id,
            quantity_sold,
            total_sales_value,
            transaction_time
        FROM base
        WHERE
            transaction_id IS NOT NULL
            AND TRIM(transaction_id) <> ''
            AND quantity_sold IS NOT NULL
            AND quantity_sold > 0
            AND total_sales_value IS NOT NULL
            AND total_sales_value > 0
    ),
    ranked AS (
        SELECT
            transaction_id,
            date,
            product_id,
            store_id,
            quantity_sold,
            total_sales_value,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        transaction_id,
        date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_value
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

job.commit()