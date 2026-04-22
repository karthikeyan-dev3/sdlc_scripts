```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark session
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# CSV read options (adjust if your silver outputs use different conventions)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE"
}

# ===================================================================================
# SOURCE TABLES (Read from S3) + TEMP VIEWS
# ===================================================================================

# ---- silver.orders_silver
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)
orders_silver_df.createOrReplaceTempView("orders_silver")

# ---- silver.order_items_silver
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# ---- silver.products_silver
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

# ---- silver.daily_order_summary_silver
daily_order_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/daily_order_summary_silver.{FILE_FORMAT}/")
)
daily_order_summary_silver_df.createOrReplaceTempView("daily_order_summary_silver")

# ===================================================================================
# TARGET TABLE: gold_orders
# Grain: one row per order_id
# ===================================================================================
gold_orders_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(os.order_id AS STRING)                         AS order_id,
        CAST(os.order_date AS DATE)                         AS order_date,
        CAST(os.order_status AS STRING)                     AS order_status,
        CAST(os.customer_id AS STRING)                      AS customer_id,
        CAST(os.order_total_amount AS DECIMAL(38, 10))      AS order_total_amount,
        CAST(os.currency_code AS STRING)                    AS currency_code,
        CAST(os.source_system AS STRING)                    AS source_system,
        CAST(os.ingestion_date AS DATE)                     AS ingestion_date
    FROM orders_silver os
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM base
)
SELECT
    order_id,
    order_date,
    order_status,
    customer_id,
    order_total_amount,
    currency_code,
    source_system,
    ingestion_date
FROM dedup
WHERE rn = 1
""")

(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold_order_items
# Grain: one row per order_item_id within order_id
# ===================================================================================
gold_order_items_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(ois.order_id AS STRING)                    AS order_id,
        CAST(ois.order_item_id AS STRING)               AS order_item_id,
        CAST(ois.product_id AS STRING)                  AS product_id,
        CAST(ois.quantity AS INT)                       AS quantity,
        CAST(ois.unit_price AS DECIMAL(38, 10))         AS unit_price,
        CAST(ois.line_amount AS DECIMAL(38, 10))        AS line_amount
    FROM order_items_silver ois
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY order_item_id
        ) AS rn
    FROM base
)
SELECT
    order_id,
    order_item_id,
    product_id,
    quantity,
    unit_price,
    line_amount
FROM dedup
WHERE rn = 1
""")

(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: gold_products
# Grain: one row per product_id
# ===================================================================================
gold_products_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(ps.product_id AS STRING)           AS product_id,
        CAST(ps.product_name AS STRING)         AS product_name,
        CAST(ps.product_category AS STRING)     AS product_category,
        CAST(ps.product_status AS STRING)       AS product_status
    FROM products_silver ps
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    product_category,
    product_status
FROM dedup
WHERE rn = 1
""")

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_products.csv")
)

# ===================================================================================
# TARGET TABLE: gold_daily_order_summary
# Grain: one row per order_date
# ===================================================================================
gold_daily_order_summary_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(doss.order_date AS DATE)                    AS order_date,
        CAST(doss.orders_count AS INT)                   AS orders_count,
        CAST(doss.unique_customers_count AS INT)         AS unique_customers_count,
        CAST(doss.gross_sales_amount AS DECIMAL(38, 10)) AS gross_sales_amount
    FROM daily_order_summary_silver doss
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_date
            ORDER BY order_date
        ) AS rn
    FROM base
)
SELECT
    order_date,
    orders_count,
    unique_customers_count,
    gross_sales_amount
FROM dedup
WHERE rn = 1
""")

(
    gold_daily_order_summary_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_daily_order_summary.csv")
)

# ===================================================================================
# TARGET TABLE: gold_customers
# Not generated: no provided silver customers source table in UDT config
# ===================================================================================

job.commit()
```