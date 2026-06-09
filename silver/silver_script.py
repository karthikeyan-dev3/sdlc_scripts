import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read source tables from S3
# -----------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
    SELECT
        TRIM(pb.product_id) AS product_id,
        NULLIF(TRIM(pb.product_name), '') AS product_name,
        NULLIF(TRIM(pb.category), '') AS category,
        COALESCE(CAST(pb.is_active AS BOOLEAN), TRUE) AS is_active_bool
    FROM products_bronze pb
),
filtered AS (
    SELECT
        product_id,
        product_name,
        category
    FROM base
    WHERE product_id IS NOT NULL
      AND is_active_bool = TRUE
),
dedup AS (
    SELECT
        product_id,
        MAX(product_name) AS product_name,
        MAX(category) AS category
    FROM filtered
    GROUP BY product_id
)
SELECT
    product_id,
    product_name,
    category
FROM dedup
"""

products_silver_df = spark.sql(products_silver_sql)
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
# ============================================================
stores_silver_sql = """
WITH base AS (
    SELECT
        TRIM(sb.store_id) AS store_id,
        NULLIF(TRIM(sb.store_name), '') AS store_name,
        NULLIF(TRIM(sb.city), '') AS city,
        NULLIF(TRIM(sb.store_type), '') AS store_type
    FROM stores_bronze sb
),
filtered AS (
    SELECT
        store_id,
        store_name,
        city,
        store_type
    FROM base
    WHERE store_id IS NOT NULL
),
dedup AS (
    SELECT
        store_id,
        MAX(store_name) AS store_name,
        MAX(city) AS city,
        MAX(store_type) AS store_type
    FROM filtered
    GROUP BY store_id
)
SELECT
    store_id,
    store_name,
    city,
    store_type
FROM dedup
"""

stores_silver_df = spark.sql(stores_silver_sql)
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
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.sale_amount AS DOUBLE) AS total_amount,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        stb.transaction_time AS transaction_time_ts
    FROM sales_transactions_bronze stb
),
filtered AS (
    SELECT
        transaction_id,
        transaction_date,
        total_amount,
        store_id,
        product_id,
        transaction_time_ts
    FROM base
    WHERE transaction_id IS NOT NULL
      AND stb_transaction_time_present(transaction_time_ts)
      AND total_amount >= 0
),
conformed AS (
    SELECT
        f.transaction_id,
        f.transaction_date,
        f.total_amount,
        f.store_id,
        f.product_id,
        f.transaction_time_ts
    FROM filtered f
    LEFT JOIN stores_silver ss
        ON f.store_id = ss.store_id
    LEFT JOIN products_silver ps
        ON f.product_id = ps.product_id
),
ranked AS (
    SELECT
        transaction_id,
        transaction_date,
        total_amount,
        store_id,
        product_id,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time_ts DESC, total_amount DESC
        ) AS rn
    FROM conformed
    WHERE transaction_time_ts IS NOT NULL
),
dedup AS (
    SELECT
        transaction_id,
        transaction_date,
        total_amount,
        store_id,
        product_id
    FROM ranked
    WHERE rn = 1
)
SELECT
    transaction_id,
    transaction_date,
    total_amount,
    store_id,
    product_id
FROM dedup
"""

# Helper UDF-free workaround: inline check is done in SQL via WHERE transaction_time_ts IS NOT NULL.
# To keep only allowed SQL functions, we avoid extra functions and rely on IS NOT NULL in SQL above.
sales_transactions_silver_sql = sales_transactions_silver_sql.replace(
    "AND stb_transaction_time_present(transaction_time_ts)",
    ""
)

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
