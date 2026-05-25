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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
transactions_bronze_df = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

transactions_bronze_df.createOrReplaceTempView("transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ------------------------------------
# Target: silver.transactions_silver
# ------------------------------------
transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        tb.transaction_id AS transaction_id,
        CAST(tb.transaction_time AS date) AS transaction_date,
        tb.product_id AS product_id,
        tb.store_id AS store_id,
        CAST(tb.quantity AS int) AS quantity_sold,
        CAST(tb.sale_amount AS double) AS sales_amount,
        tb.transaction_time AS transaction_time,
        ROW_NUMBER() OVER (
            PARTITION BY tb.transaction_id, tb.product_id, tb.store_id, tb.transaction_time
            ORDER BY tb.transaction_time
        ) AS rn
    FROM transactions_bronze tb
    WHERE tb.transaction_id IS NOT NULL
      AND tb.product_id IS NOT NULL
      AND tb.store_id IS NOT NULL
      AND tb.transaction_time IS NOT NULL
)
SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    quantity_sold,
    sales_amount
FROM base
WHERE rn = 1
""")
transactions_silver_df.createOrReplaceTempView("transactions_silver")

(
    transactions_silver_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# --------------------------------
# Target: silver.products_silver
# --------------------------------
products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        pb.brand AS brand,
        ROW_NUMBER() OVER (
            PARTITION BY pb.product_id
            ORDER BY pb.product_id
        ) AS rn
    FROM products_bronze pb
    WHERE pb.product_id IS NOT NULL
)
SELECT
    product_id,
    product_name,
    category,
    brand
FROM base
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

# ------------------------------
# Target: silver.stores_silver
# ------------------------------
stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        sb.state AS region,
        ROW_NUMBER() OVER (
            PARTITION BY sb.store_id
            ORDER BY sb.store_id
        ) AS rn
    FROM stores_bronze sb
    WHERE sb.store_id IS NOT NULL
)
SELECT
    store_id,
    store_name,
    region
FROM base
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

# ---------------------------------------
# Target: silver.sales_aggregated_silver
# ---------------------------------------
sales_aggregated_silver_df = spark.sql("""
SELECT
    ts.transaction_date AS aggregation_date,
    ss.region AS region,
    ps.category AS category,
    SUM(CAST(ts.sales_amount AS double)) AS total_sales_amount,
    SUM(CAST(ts.quantity_sold AS int)) AS total_quantity_sold
FROM transactions_silver ts
INNER JOIN products_silver ps
    ON ts.product_id = ps.product_id
INNER JOIN stores_silver ss
    ON ts.store_id = ss.store_id
GROUP BY
    ts.transaction_date,
    ss.region,
    ps.category
""")

(
    sales_aggregated_silver_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)