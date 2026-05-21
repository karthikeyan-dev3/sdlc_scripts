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

# ------------------------------------------------------------
# SOURCE: bronze.sales_transactions_bronze -> TARGET: silver.sales_transaction_silver
# ------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transaction_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(UPPER(stb.transaction_id)) AS transaction_id,
        TRIM(UPPER(stb.product_id))     AS product_id,
        TRIM(UPPER(stb.store_id))       AS store_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.quantity AS INT)          AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE)    AS revenue,
        stb.transaction_time               AS transaction_time
    FROM sales_transactions_bronze stb
),
filtered AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        quantity_sold,
        revenue,
        transaction_time
    FROM base
    WHERE transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
      AND product_id IS NOT NULL AND TRIM(product_id) <> ''
      AND store_id IS NOT NULL AND TRIM(store_id) <> ''
      AND quantity_sold > 0
      AND revenue >= 0
),
deduped AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        quantity_sold,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
    FROM filtered
)
SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    quantity_sold,
    revenue
FROM deduped
WHERE rn = 1
""")
sales_transaction_silver_df.createOrReplaceTempView("sales_transaction_silver")

(
    sales_transaction_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transaction_silver.csv")
)

# ------------------------------------------------------------
# SOURCE: bronze.products_bronze -> TARGET: silver.product_silver
# ------------------------------------------------------------
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

product_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(UPPER(pb.product_id)) AS product_id,
        TRIM(pb.product_name)      AS product_name,
        TRIM(UPPER(pb.category))   AS category,
        CAST(pb.price AS DOUBLE)   AS price,
        pb.is_active               AS is_active,
        pb.product_name            AS product_name_raw,
        pb.category                AS category_raw,
        pb.price                   AS price_raw
    FROM products_bronze pb
),
filtered AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        CAST(NULL AS INT) AS inventory_level
    FROM base
    WHERE (is_active IS NULL OR LOWER(TRIM(CAST(is_active AS STRING))) <> 'false')
      AND product_id IS NOT NULL AND TRIM(product_id) <> ''
      AND price IS NOT NULL AND price >= 0
),
ranked AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        inventory_level,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM filtered
)
SELECT
    product_id,
    product_name,
    category,
    price,
    inventory_level
FROM ranked
WHERE rn = 1
""")
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------
# SOURCE: bronze.stores_bronze -> TARGET: silver.store_silver
# ------------------------------------------------------------
stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

store_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(UPPER(sb.store_id))   AS store_id,
        TRIM(sb.store_name)        AS store_name,
        TRIM(UPPER(sb.state))      AS region,
        CAST(NULL AS INT)          AS store_capacity,
        sb.store_name              AS store_name_raw,
        sb.state                   AS state_raw
    FROM stores_bronze sb
),
filtered AS (
    SELECT
        store_id,
        store_name,
        region,
        store_capacity
    FROM base
    WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
),
ranked AS (
    SELECT
        store_id,
        store_name,
        region,
        store_capacity,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY
                CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN region IS NOT NULL AND TRIM(region) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM filtered
)
SELECT
    store_id,
    store_name,
    region,
    store_capacity
FROM ranked
WHERE rn = 1
""")
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ------------------------------------------------------------
# SOURCE: silver.sales_transaction_silver LEFT JOIN silver.product_silver -> TARGET: silver.performance_aggregates_silver
# ------------------------------------------------------------
performance_aggregates_silver_df = spark.sql("""
SELECT
    sts.store_id AS store_id,
    sts.product_id AS product_id,
    sts.transaction_date AS date,
    SUM(sts.revenue) AS total_revenue,
    COUNT(DISTINCT sts.transaction_id) AS total_transactions,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    CASE
        WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.revenue) / SUM(sts.quantity_sold)
        ELSE ps.price
    END AS average_price
FROM sales_transaction_silver sts
LEFT JOIN product_silver ps
    ON sts.product_id = ps.product_id
WHERE sts.store_id IS NOT NULL
  AND sts.product_id IS NOT NULL
  AND sts.transaction_date IS NOT NULL
GROUP BY
    sts.store_id,
    sts.product_id,
    sts.transaction_date,
    ps.price
""")

(
    performance_aggregates_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/performance_aggregates_silver.csv")
)

job.commit()
