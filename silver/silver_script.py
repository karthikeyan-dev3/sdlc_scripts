import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ============================================================
# Read Source Tables (Bronze) and Create Temp Views
# ============================================================

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

# ============================================================
# Target: silver.products_silver
# Columns: product_id, product_name, category
# ============================================================

products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING)   AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING)     AS category
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category
    FROM dedup
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ============================================================
# Target: silver.stores_silver
# Columns: store_id, store_name, city, store_type
# ============================================================

stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING)     AS store_id,
            CAST(TRIM(sb.store_name) AS STRING)   AS store_name,
            CAST(TRIM(sb.city) AS STRING)         AS city,
            CAST(TRIM(sb.store_type) AS STRING)   AS store_type
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            city,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN city IS NOT NULL AND city <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_type IS NOT NULL AND store_type <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        city,
        store_type
    FROM dedup
    WHERE rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# Target: silver.sales_transactions_silver
# Columns: transaction_id, product_id, store_id, transaction_time, revenue, quantity, date
# ============================================================

sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING)                   AS transaction_id,
            CAST(TRIM(stb.product_id) AS STRING)                       AS product_id,
            CAST(TRIM(stb.store_id) AS STRING)                         AS store_id,
            CAST(stb.transaction_time AS TIMESTAMP)                    AS transaction_time,
            CAST(stb.sale_amount AS DOUBLE)                            AS revenue,
            CAST(stb.quantity AS INT)                                  AS quantity,
            CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE)      AS date
        FROM sales_transactions_bronze stb
        INNER JOIN products_silver ps
            ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
        INNER JOIN stores_silver ss
            ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
        WHERE
            TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
            AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
            AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
            AND CAST(stb.quantity AS INT) > 0
            AND CAST(stb.sale_amount AS DOUBLE) >= 0
    ),
    dedup AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            transaction_time,
            revenue,
            quantity,
            date,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_time,
        revenue,
        quantity,
        date
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