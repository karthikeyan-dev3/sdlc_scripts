```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Context
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Common CSV read options (Bronze is CSV)
# -----------------------------------------------------------------------------------
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\"",
}

# ===================================================================================
# 1) customers_silver
#    - Clean + dedup latest record per customer_id
#    - Standardize customer_id (TRIM + UPPER)
#    - Filter null/blank customer_id
#    - ROW_NUMBER dedup
# ===================================================================================

customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)
customers_bronze_df.createOrReplaceTempView("customers_bronze")

customers_silver_sql = """
WITH base AS (
    SELECT
        UPPER(TRIM(CAST(customer_id AS STRING))) AS customer_id,
        *,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(CAST(customer_id AS STRING)))
            ORDER BY
                DATE(COALESCE(s3_ingest_date, CURRENT_DATE())) DESC
        ) AS rn
    FROM customers_bronze
    WHERE customer_id IS NOT NULL
      AND TRIM(CAST(customer_id AS STRING)) <> ''
),
dedup AS (
    SELECT
        customer_id
    FROM base
    WHERE rn = 1
)
SELECT
    customer_id
FROM dedup
"""

customers_silver_df = spark.sql(customers_silver_sql)

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

customers_silver_df.createOrReplaceTempView("customers_silver")

# ===================================================================================
# 2) products_silver
#    - Clean + dedup latest active record per product_id
#    - Standardize product_id (TRIM + UPPER)
#    - Clean price (non-negative; else NULL)
#    - Filter null/blank product_id
#    - ROW_NUMBER dedup
# ===================================================================================

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_sql = """
WITH base AS (
    SELECT
        UPPER(TRIM(CAST(product_id AS STRING))) AS product_id,
        CASE
            WHEN CAST(price AS DECIMAL(18,2)) < 0 THEN NULL
            ELSE CAST(price AS DECIMAL(18,2))
        END AS price,
        *,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(CAST(product_id AS STRING)))
            ORDER BY
                -- prefer active if present; then latest ingest date
                COALESCE(CAST(is_active AS INT), 1) DESC,
                DATE(COALESCE(s3_ingest_date, CURRENT_DATE())) DESC
        ) AS rn
    FROM products_bronze
    WHERE product_id IS NOT NULL
      AND TRIM(CAST(product_id AS STRING)) <> ''
),
dedup AS (
    SELECT
        product_id,
        price
    FROM base
    WHERE rn = 1
)
SELECT
    product_id,
    price
FROM dedup
"""

products_silver_df = spark.sql(products_silver_sql)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# ===================================================================================
# 3) order_status_silver
#    - Canonical status mapping
#    - Normalize to approved set (completed/cancelled/returned/pending)
#    - Filter invalid/null
#    - De-duplicate (keep one row per normalized status)
# ===================================================================================

order_status_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/order_status_bronze.{FILE_FORMAT}/")
)
order_status_bronze_df.createOrReplaceTempView("order_status_bronze")

order_status_silver_sql = """
WITH normalized AS (
    SELECT
        CASE
            WHEN LOWER(TRIM(CAST(order_status AS STRING))) IN ('completed','complete','shipped','delivered','fulfilled')
                THEN 'completed'
            WHEN LOWER(TRIM(CAST(order_status AS STRING))) IN ('cancelled','canceled','void','voided')
                THEN 'cancelled'
            WHEN LOWER(TRIM(CAST(order_status AS STRING))) IN ('returned','refunded','return')
                THEN 'returned'
            WHEN LOWER(TRIM(CAST(order_status AS STRING))) IN ('pending','in_progress','processing','new','open')
                THEN 'pending'
            ELSE NULL
        END AS order_status
    FROM order_status_bronze
    WHERE order_status IS NOT NULL
      AND TRIM(CAST(order_status AS STRING)) <> ''
),
dedup AS (
    SELECT
        order_status,
        ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY order_status) AS rn
    FROM normalized
    WHERE order_status IS NOT NULL
)
SELECT
    order_status
FROM dedup
WHERE rn = 1
"""

order_status_silver_df = spark.sql(order_status_silver_sql)

(
    order_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_status_silver.csv")
)

order_status_silver_df.createOrReplaceTempView("order_status_silver")

# ===================================================================================
# 4) customer_orders_silver
#    - Join to customers_silver/products_silver/order_status_silver
#    - Standardize ids; cast order_date + s3_ingest_date to DATE
#    - Validate quantity (>0) and unit_price (>=0)
#    - order_amount = quantity * unit_price (after validation)
#    - order_status normalized via order_status_silver
#    - Dedup latest per (order_id, product_id) by s3_ingest_date then etl_load_ts
# ===================================================================================

customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_orders_silver_sql = """
WITH joined AS (
    SELECT
        -- cos.order_id = cob.order_id
        UPPER(TRIM(CAST(cob.order_id AS STRING))) AS order_id,

        -- cos.order_date = cob.order_date
        DATE(CAST(cob.order_date AS STRING)) AS order_date,

        -- cos.customer_id = cs.customer_id (via join)
        cs.customer_id AS customer_id,

        -- cos.product_id = ps.product_id (via join)
        ps.product_id AS product_id,

        -- cos.quantity = cob.quantity (validated)
        CASE
            WHEN CAST(cob.quantity AS INT) > 0 THEN CAST(cob.quantity AS INT)
            ELSE NULL
        END AS quantity,

        -- cos.unit_price = cob.unit_price (validated)
        CASE
            WHEN CAST(cob.unit_price AS DECIMAL(18,2)) >= 0 THEN CAST(cob.unit_price AS DECIMAL(18,2))
            ELSE NULL
        END AS unit_price,

        -- cos.order_status = oss.order_status (via join)
        oss.order_status AS order_status,

        -- cos.s3_ingest_date = cob.s3_ingest_date
        DATE(CAST(cob.s3_ingest_date AS STRING)) AS s3_ingest_date,

        -- cos.etl_load_ts = cob.etl_load_ts
        CAST(cob.etl_load_ts AS TIMESTAMP) AS etl_load_ts
    FROM customer_orders_bronze cob
    LEFT JOIN customers_silver cs
        ON UPPER(TRIM(CAST(cob.customer_id AS STRING))) = cs.customer_id
    LEFT JOIN products_silver ps
        ON UPPER(TRIM(CAST(cob.product_id AS STRING))) = ps.product_id
    LEFT JOIN order_status_silver oss
        ON LOWER(TRIM(CAST(cob.order_status AS STRING))) = oss.order_status
),
amounts AS (
    SELECT
        order_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        unit_price,
        CAST(quantity * unit_price AS DECIMAL(18,2)) AS order_amount,
        order_status,
        s3_ingest_date,
        etl_load_ts
    FROM joined
    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
      AND product_id IS NOT NULL AND TRIM(product_id) <> ''
      AND customer_id IS NOT NULL AND TRIM(customer_id) <> ''
      AND order_date IS NOT NULL
      AND quantity IS NOT NULL
      AND unit_price IS NOT NULL
      AND order_status IS NOT NULL AND TRIM(order_status) <> ''
      AND s3_ingest_date IS NOT NULL
      AND etl_load_ts IS NOT NULL
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id
            ORDER BY s3_ingest_date DESC, etl_load_ts DESC
        ) AS rn
    FROM amounts
)
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    order_amount,
    order_status,
    s3_ingest_date,
    etl_load_ts
FROM dedup
WHERE rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
```