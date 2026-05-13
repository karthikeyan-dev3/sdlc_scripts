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

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------------
# 2) product_master_silver
#    - Deduplicate on product_id
#    - Standardize casing & trimming for product_name/category/brand
#    - Enforce numeric types for price
#    - Filter to active products (is_active = true)
#    - Derived cost not available -> not included (no target column provided in UDT)
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            TRIM(UPPER(pb.product_name)) AS product_name,
            TRIM(UPPER(pb.category)) AS category,
            TRIM(UPPER(pb.brand)) AS brand,
            CAST(pb.price AS FLOAT) AS price,
            CAST(pb.is_active AS BOOLEAN) AS is_active
        FROM products_bronze pb
        WHERE CAST(pb.is_active AS BOOLEAN) = TRUE
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN brand IS NOT NULL AND brand <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        is_active
    FROM dedup
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------------
# 3) store_master_silver
#    - Deduplicate on store_id
#    - Standardize trimming & casing for store_name/city/state
#    - Derive location = CONCAT(city, ', ', state)
#    - Rename open_date -> opening_date (cast to DATE)
#    - Retain store_type
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            TRIM(UPPER(sb.store_name)) AS store_name,
            TRIM(UPPER(sb.city)) AS city,
            TRIM(UPPER(sb.state)) AS state,
            CAST(sb.store_type AS STRING) AS store_type,
            CAST(sb.open_date AS DATE) AS opening_date,
            CONCAT(TRIM(UPPER(sb.city)), ', ', TRIM(UPPER(sb.state))) AS location
        FROM stores_bronze sb
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN city IS NOT NULL AND city <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN state IS NOT NULL AND state <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_type IS NOT NULL AND store_type <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN opening_date IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        opening_date,
        location
    FROM dedup
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------------------------------
# 4) sales_transactions_silver
#    - Deduplicate on transaction_id
#    - transaction_date = CAST(transaction_time AS DATE)
#    - quantity_sold = quantity
#    - total_sales_amount = sale_amount
#    - discount_amount = 0.0
#    - net_sales_amount = sale_amount - 0.0
#    - Enforce non-negative quantity and amounts
#    - Ensure referential integrity: keep records with valid product_id/store_id
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(stb.store_id AS STRING) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
            CAST(0.0 AS DOUBLE) AS discount_amount,
            CAST(CAST(stb.sale_amount AS DOUBLE) - 0.0 AS DOUBLE) AS net_sales_amount
        FROM sales_transactions_bronze stb
        LEFT JOIN product_master_silver pms
            ON CAST(stb.product_id AS STRING) = pms.product_id
        LEFT JOIN store_master_silver sms
            ON CAST(stb.store_id AS STRING) = sms.store_id
        WHERE pms.product_id IS NOT NULL
          AND sms.store_id IS NOT NULL
    ),
    validated AS (
        SELECT
            *
        FROM joined
        WHERE COALESCE(quantity_sold, 0) >= 0
          AND COALESCE(total_sales_amount, 0.0) >= 0.0
          AND COALESCE(discount_amount, 0.0) >= 0.0
          AND COALESCE(net_sales_amount, 0.0) >= 0.0
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    CASE WHEN transaction_date IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN total_sales_amount IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN net_sales_amount IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM validated
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount,
        discount_amount,
        net_sales_amount
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

job.commit()