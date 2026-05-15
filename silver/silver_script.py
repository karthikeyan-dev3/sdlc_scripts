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
# 1) Read source tables from S3
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
# 2) Create temp views
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------
# products_silver: transform + dedup
# ------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        UPPER(TRIM(pb.category)) AS category,
        UPPER(TRIM(pb.brand)) AS brand,
        CAST(NULL AS float) AS cost_price,
        CAST(pb.price AS float) AS selling_price
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
        AND TRIM(pb.product_name) IS NOT NULL
        AND TRIM(pb.product_name) <> ''
    ),
    ranked AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        cost_price,
        selling_price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN brand IS NOT NULL AND brand <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN selling_price IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      cost_price,
      selling_price
    FROM ranked
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

# ----------------------------------
# stores_silver: transform + dedup
# ----------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        CAST(NULL AS string) AS store_manager,
        CAST(NULL AS string) AS store_size
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    ),
    ranked AS (
      SELECT
        store_id,
        store_name,
        location,
        store_manager,
        store_size,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN location IS NOT NULL AND location <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      location,
      store_manager,
      store_size
    FROM ranked
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

# ----------------------------------------------------
# sales_transactions_silver: conform + transform + dedup
# ----------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.sale_amount AS double) AS total_revenue,
        stb.transaction_time AS transaction_time
      FROM sales_transactions_bronze stb
      INNER JOIN stores_silver ss
        ON TRIM(stb.store_id) = ss.store_id
      INNER JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
        AND CAST(stb.quantity AS int) > 0
        AND CAST(stb.sale_amount AS double) >= 0
    ),
    ranked AS (
      SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      transaction_date,
      store_id,
      product_id,
      quantity_sold,
      total_revenue
    FROM ranked
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