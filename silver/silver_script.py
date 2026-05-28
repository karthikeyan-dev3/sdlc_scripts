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

# =========================
# Read Source Tables (Bronze)
# =========================
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

# =========================
# Target Table: products_silver
# =========================
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS FLOAT) AS price
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
          AND TRIM(pb.product_id) <> ''
          AND (pb.is_active IS NULL OR LOWER(TRIM(CAST(pb.is_active AS STRING))) = 'true')
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM ranked
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

products_silver_out = TARGET_PATH + "/products_silver.csv"
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(products_silver_out)
)

# =========================
# Target Table: stores_silver
# =========================
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id) AS store_id,
            TRIM(CONCAT(TRIM(sb.city), ', ', TRIM(sb.state))) AS store_location,
            TRIM(sb.store_type) AS store_type
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    ),
    ranked AS (
        SELECT
            store_id,
            store_location,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_location IS NOT NULL AND store_location <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_type IS NOT NULL AND store_type <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_location,
        store_type
    FROM ranked
    WHERE rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

stores_silver_out = TARGET_PATH + "/stores_silver.csv"
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(stores_silver_out)
)

# =========================
# Target Table: sales_transactions_silver
# =========================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.sale_amount AS DOUBLE) AS revenue,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            CAST(stb.quantity AS INT) AS quantity_sold,
            TRIM(ps.category) AS category
        FROM sales_transactions_bronze stb
        INNER JOIN products_silver ps
            ON TRIM(stb.product_id) = TRIM(ps.product_id)
        WHERE TRIM(stb.transaction_id) IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
          AND TRIM(stb.product_id) IS NOT NULL
          AND TRIM(stb.product_id) <> ''
          AND TRIM(stb.store_id) IS NOT NULL
          AND TRIM(stb.store_id) <> ''
          AND (CAST(stb.sale_amount AS DOUBLE) IS NULL OR CAST(stb.sale_amount AS DOUBLE) >= 0)
          AND (CAST(stb.quantity AS INT) IS NULL OR CAST(stb.quantity AS INT) >= 0)
    ),
    ranked AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            revenue,
            transaction_date,
            quantity_sold,
            category,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    CASE WHEN product_id IS NOT NULL AND product_id <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_id IS NOT NULL AND store_id <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN transaction_date IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN quantity_sold IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        revenue,
        transaction_date,
        quantity_sold,
        category
    FROM ranked
    WHERE rn = 1
    """
)

sales_transactions_silver_out = TARGET_PATH + "/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_transactions_silver_out)
)