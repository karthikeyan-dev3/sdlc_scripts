import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3 and create temp views
# ------------------------------------------------------------

products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# 2) products_silver
#    - trims/case-normalizes text fields
#    - enforces non-null product_id
#    - de-duplicates by product_id
# ------------------------------------------------------------

products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        CAST(pb.price AS FLOAT) AS price
    FROM products_bronze pb
    WHERE TRIM(pb.product_id) IS NOT NULL
      AND TRIM(pb.product_id) <> ''
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 1 ELSE 0 END ASC,
                CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END ASC,
                CASE WHEN price IS NULL THEN 1 ELSE 0 END ASC
        ) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    category,
    price
FROM dedup
WHERE rn = 1
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# 3) stores_silver
#    - trims/case-normalizes text fields
#    - region derived from state per UDT transformation (region=sb.state)
#    - store_manager derived from store_type per UDT transformation (store_manager=sb.store_type)
#    - enforces non-null store_id
#    - de-duplicates by store_id
# ------------------------------------------------------------

stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.state) AS region,
        TRIM(sb.store_type) AS store_manager
    FROM stores_bronze sb
    WHERE TRIM(sb.store_id) IS NOT NULL
      AND TRIM(sb.store_id) <> ''
),
dedup AS (
    SELECT
        store_id,
        store_name,
        region,
        store_manager,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY
                CASE WHEN store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END ASC,
                CASE WHEN region IS NULL OR TRIM(region) = '' THEN 1 ELSE 0 END ASC,
                CASE WHEN store_manager IS NULL OR TRIM(store_manager) = '' THEN 1 ELSE 0 END ASC
        ) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    region,
    store_manager
FROM dedup
WHERE rn = 1
""")
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# 4) sales_transactions_silver
#    - transaction_date = CAST(transaction_time AS DATE)
#    - total_amount = sale_amount
#    - validates quantity > 0 and total_amount >= 0
#    - enforces non-null transaction_id/store_id/product_id
#    - joins to products_silver and stores_silver for referential integrity
#    - de-duplicates by transaction_id keeping latest transaction_time
# ------------------------------------------------------------

sales_transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS transaction_date,
        CAST(stb.sale_amount AS DOUBLE) AS total_amount,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
    FROM sales_transactions_bronze stb
    WHERE TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
      AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
      AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
      AND CAST(stb.quantity AS INT) > 0
      AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
conformed AS (
    SELECT
        b.transaction_id,
        b.product_id,
        b.store_id,
        b.transaction_date,
        b.total_amount,
        b.quantity,
        b.transaction_time_ts
    FROM base b
    INNER JOIN products_silver ps
        ON b.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON b.store_id = ss.store_id
),
dedup AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        total_amount,
        quantity,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time_ts DESC
        ) AS rn
    FROM conformed
)
SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    total_amount,
    quantity
FROM dedup
WHERE rn = 1
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------
# 5) aggregated_sales_silver
#    - total_revenue = SUM(total_amount)
#    - total_quantity_sold = SUM(quantity)
#    - transaction_count = COUNT(DISTINCT transaction_id)
#    - grouped by store_id, product_id, transaction_date
# ------------------------------------------------------------

aggregated_sales_silver_df = spark.sql("""
SELECT
    sts.store_id AS store_id,
    sts.product_id AS product_id,
    sts.transaction_date AS transaction_date,
    SUM(sts.total_amount) AS total_revenue,
    SUM(sts.quantity) AS total_quantity_sold,
    COUNT(DISTINCT sts.transaction_id) AS transaction_count
FROM sales_transactions_silver sts
GROUP BY
    sts.store_id,
    sts.product_id,
    sts.transaction_date
""")

(
    aggregated_sales_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)