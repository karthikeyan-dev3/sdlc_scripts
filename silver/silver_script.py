import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) product_master_silver
# ------------------------------------------------------------------------------------
product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pmb.product_id) AS product_id,
            TRIM(pmb.product_name) AS product_name,
            TRIM(pmb.category) AS category,
            TRIM(pmb.brand) AS brand,
            CAST(pmb.price AS DECIMAL(38, 10)) AS price,
            pmb.is_active AS is_active
        FROM product_master_bronze pmb
        WHERE TRIM(pmb.product_id) IS NOT NULL
          AND (pmb.is_active = true OR pmb.is_active IS NULL)
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
product_master_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/product_master_silver.csv"
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------------
# 2) store_master_silver
# ------------------------------------------------------------------------------------
store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(smb.store_id) AS store_id,
            TRIM(smb.store_name) AS store_name,
            TRIM(smb.city) AS city,
            TRIM(smb.state) AS state,
            TRIM(smb.store_type) AS store_type
        FROM store_master_bronze smb
        WHERE TRIM(smb.store_id) IS NOT NULL
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            CONCAT(city, ', ', state) AS location,
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
        store_type,
        CAST(NULL AS STRING) AS store_manager
    FROM ranked
    WHERE rn = 1
    """
)
store_master_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/store_master_silver.csv"
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------------------------------
# 3) sales_transactions_silver
# ------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount
        FROM sales_transactions_bronze stb
        WHERE TRIM(stb.transaction_id) IS NOT NULL
    ),
    validated AS (
        SELECT
            transaction_id,
            transaction_date,
            product_id,
            store_id,
            CASE
                WHEN quantity_sold < 0 THEN NULL
                ELSE quantity_sold
            END AS quantity_sold,
            CASE
                WHEN total_sales_amount < 0 THEN NULL
                ELSE total_sales_amount
            END AS total_sales_amount
        FROM base
    ),
    joined AS (
        SELECT
            v.transaction_id,
            v.transaction_date,
            v.product_id,
            v.store_id,
            v.quantity_sold,
            v.total_sales_amount
        FROM validated v
        LEFT JOIN product_master_silver pms
            ON v.product_id = pms.product_id
        LEFT JOIN store_master_silver sms
            ON v.store_id = sms.store_id
    ),
    ranked AS (
        SELECT
            transaction_id,
            transaction_date,
            product_id,
            store_id,
            quantity_sold,
            total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    CASE WHEN transaction_date IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN quantity_sold IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN total_sales_amount IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------------
# 4) aggregated_sales_silver
# ------------------------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.transaction_date AS transaction_date,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_sales_amount) AS total_sales_amount,
        CASE
            WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.total_sales_amount) / SUM(sts.quantity_sold)
            ELSE NULL
        END AS average_price
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.transaction_date
    """
)
aggregated_sales_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/aggregated_sales_silver.csv"
)