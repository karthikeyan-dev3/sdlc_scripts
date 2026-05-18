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

# -----------------------------
# Read source tables (Bronze)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
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

# -----------------------------
# Create temp views (Bronze)
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            CASE
                WHEN stb.quantity IS NULL OR CAST(stb.quantity AS INT) < 0 THEN 0
                ELSE CAST(stb.quantity AS INT)
            END AS quantity_sold,
            CASE
                WHEN stb.sale_amount IS NULL OR CAST(stb.sale_amount AS DOUBLE) < 0 THEN 0
                ELSE CAST(stb.sale_amount AS DOUBLE)
            END AS total_sales_amount,
            CASE
                WHEN stb.transaction_id IS NOT NULL
                 AND stb.product_id IS NOT NULL
                 AND stb.store_id IS NOT NULL
                 AND stb.transaction_time IS NOT NULL
                 AND CAST(stb.quantity AS INT) > 0
                 AND CAST(stb.sale_amount AS DOUBLE) >= 0
                THEN true ELSE false
            END AS valid_transaction_flag,
            CURRENT_TIMESTAMP AS processed_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount,
        valid_transaction_flag,
        processed_timestamp
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ============================================================
# Target: silver.product_silver
# ============================================================
product_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            pb.product_id AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            TRIM(pb.brand) AS brand,
            CASE
                WHEN pb.price IS NULL OR CAST(pb.price AS DOUBLE) < 0 THEN NULL
                ELSE CAST(pb.price AS DOUBLE)
            END AS price,
            COALESCE(CAST(pb.is_active AS BOOLEAN), true) AS is_active
        FROM products_bronze pb
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            price,
            is_active,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN is_active = true THEN 0 ELSE 1 END ASC,
                    CASE WHEN price IS NULL THEN 1 ELSE 0 END ASC,
                    price DESC
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

# ============================================================
# Target: silver.store_silver
# ============================================================
store_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sb.store_id AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            CASE WHEN sb.city IS NOT NULL AND sb.state IS NOT NULL THEN true ELSE false END AS verified_location_flag,
            TRIM(sb.store_type) AS store_type,
            sb.open_date AS opening_date,
            CASE
                WHEN LOWER(TRIM(sb.store_type)) LIKE '%franchise%' THEN true
                ELSE false
            END AS is_franchise
        FROM stores_bronze sb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            verified_location_flag,
            store_type,
            opening_date,
            is_franchise,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 0 ELSE 1 END ASC,
                    CASE WHEN verified_location_flag = true THEN 0 ELSE 1 END ASC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        verified_location_flag,
        store_type,
        opening_date,
        is_franchise
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

# ============================================================
# Target: silver.sales_aggregates_silver
# ============================================================
sales_aggregates_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS aggregation_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_sales_amount) AS total_sales_amount,
        CASE
            WHEN SUM(sts.quantity_sold) = 0 THEN NULL
            ELSE SUM(sts.total_sales_amount) / SUM(sts.quantity_sold)
        END AS average_sales_price
    FROM sales_transactions_silver sts
    INNER JOIN product_silver ps
        ON sts.product_id = ps.product_id
    WHERE
        sts.valid_transaction_flag = true
        AND (ps.is_active = true OR ps.is_active IS NULL)
    GROUP BY
        sts.transaction_date,
        sts.product_id,
        sts.store_id
    """
)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_silver.csv")
)

# ============================================================
# Target: silver.metadata_tracking_silver
# ============================================================
metadata_tracking_silver_df = spark.sql(
    """
    SELECT
        CURRENT_DATE AS data_refresh_date,
        'products_raw,sales_transactions_raw,stores_raw' AS source_system,
        CASE
            WHEN MIN(CASE WHEN sts.transaction_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'SUCCESS'
            ELSE 'PARTIAL'
        END AS integration_status,
        COUNT(*) AS record_count,
        CURRENT_TIMESTAMP AS last_successful_refresh_time
    FROM sales_transactions_silver sts
    LEFT JOIN product_silver ps ON 1=1
    LEFT JOIN store_silver ss ON 1=1
    """
)
metadata_tracking_silver_df.createOrReplaceTempView("metadata_tracking_silver")

(
    metadata_tracking_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/metadata_tracking_silver.csv")
)