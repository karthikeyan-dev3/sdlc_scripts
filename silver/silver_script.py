
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue Context
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Common read options for CSV (adjust if your bronze CSVs have different settings)
# --------------------------------------------------------------------------------------
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# ======================================================================================
# 1) stores_silver
#    Source: bronze.stores_bronze (sb)
#    Transformations: TRIM/UPPER/COALESCE, dedup via ROW_NUMBER (latest by store_id)
# ======================================================================================

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CAST(TRIM(sb.city) AS STRING) AS city,
            CAST(UPPER(TRIM(sb.state)) AS STRING) AS state,
            CAST(UPPER(TRIM(sb.store_type)) AS STRING) AS store_type,
            CAST(COALESCE(NULLIF(TRIM(sb.country), ''), 'USA') AS STRING) AS country,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY COALESCE(sb.updated_at, sb.ingest_ts, sb.created_at) DESC, sb.store_id DESC
            ) AS rn
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        country
    FROM base
    WHERE rn = 1
    """
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# Create temp view for downstream joins (sales_transactions_silver)
stores_silver_df.createOrReplaceTempView("stores_silver")

# ======================================================================================
# 2) products_silver
#    Source: bronze.products_bronze (pb)
#    Transformations: TRIM/UPPER/LOWER/COALESCE, dedup via ROW_NUMBER (latest by product_id)
# ======================================================================================

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.brand) AS STRING) AS brand,
            CAST(UPPER(TRIM(pb.category)) AS STRING) AS category,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY COALESCE(pb.updated_at, pb.ingest_ts, pb.created_at) DESC, pb.product_id DESC
            ) AS rn
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
          AND TRIM(pb.product_id) <> ''
          AND (
                pb.is_active IS NULL
                OR CAST(pb.is_active AS STRING) = '1'
                OR LOWER(TRIM(CAST(pb.is_active AS STRING))) IN ('true', 't', 'yes', 'y')
              )
    )
    SELECT
        product_id,
        product_name,
        brand,
        category
    FROM base
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# Create temp view for downstream joins (sales_transactions_silver)
products_silver_df.createOrReplaceTempView("products_silver")

# ======================================================================================
# 3) sales_transactions_silver
#    Source: bronze.sales_transactions_bronze (stb)
#    Join: LEFT JOIN stores_silver (ss) + products_silver (ps)
#    Transformations: enforce non-null keys, dedup by transaction_id via ROW_NUMBER,
#                     normalize sales_date = DATE(transaction_time),
#                     coerce quantity/sale_amount to non-negative, keep valid dimensional refs when possible
# ======================================================================================

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,

            -- Coerce numeric values to be non-negative where possible
            CAST(
                COALESCE(
                    CASE
                        WHEN CAST(stb.quantity AS INT) < 0 THEN 0
                        ELSE CAST(stb.quantity AS INT)
                    END,
                    0
                ) AS INT
            ) AS quantity,

            CAST(
                COALESCE(
                    CASE
                        WHEN CAST(stb.sale_amount AS DECIMAL(18,2)) < 0 THEN CAST(0.00 AS DECIMAL(18,2))
                        ELSE CAST(stb.sale_amount AS DECIMAL(18,2))
                    END,
                    CAST(0.00 AS DECIMAL(18,2))
                ) AS DECIMAL(18,2)
            ) AS sale_amount,

            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sales_date,

            -- Join keys for referential checks
            ss.store_id AS dim_store_id,
            ps.product_id AS dim_product_id,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY COALESCE(stb.updated_at, stb.ingest_ts, stb.transaction_time) DESC, stb.transaction_id DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        LEFT JOIN stores_silver ss
            ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
        LEFT JOIN products_silver ps
            ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
        WHERE TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
          AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
          AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
    ),
    dedup AS (
        SELECT *
        FROM joined
        WHERE rn = 1
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        sales_date
    FROM dedup
    -- retain only records with valid dimensional references where possible
    WHERE dim_store_id IS NOT NULL
      AND dim_product_id IS NOT NULL
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)
