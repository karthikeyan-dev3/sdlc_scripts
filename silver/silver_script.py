```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
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

# Recommended CSV read options (adjust if needed)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE"
}

# ===================================================================================
# TABLE: silver.silver_dim_product
# Source: bronze.bronze_products_raw bpr
# ===================================================================================

# 1) Read source table(s) from S3
bpr_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/bronze_products_raw.{FILE_FORMAT}/")
)

# 2) Create temp views
bpr_df.createOrReplaceTempView("bronze_products_raw")

# 3) Transformation SQL (casts, trims, required keys, dedup with ROW_NUMBER)
silver_dim_product_df = spark.sql("""
WITH staged AS (
    SELECT
        CAST(TRIM(product_id) AS STRING)                AS product_id,
        CAST(TRIM(product_name) AS STRING)              AS product_name,
        CAST(TRIM(category) AS STRING)                  AS category,
        CAST(TRIM(brand) AS STRING)                     AS brand,
        CAST(price AS DECIMAL(38, 10))                  AS price,
        CAST(is_active AS BOOLEAN)                      AS is_active,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(TRIM(product_id) AS STRING)
            ORDER BY CAST(TRIM(product_id) AS STRING) DESC
        ) AS rn
    FROM bronze_products_raw
    WHERE TRIM(product_id) IS NOT NULL AND TRIM(product_id) <> ''
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active
FROM staged
WHERE rn = 1
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_dim_product.csv")
)

# ===================================================================================
# TABLE: silver.silver_dim_store
# Source: bronze.bronze_stores_raw bsr
# ===================================================================================

# 1) Read source table(s) from S3
bsr_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/bronze_stores_raw.{FILE_FORMAT}/")
)

# 2) Create temp views
bsr_df.createOrReplaceTempView("bronze_stores_raw")

# 3) Transformation SQL (casts, trims, required keys, dedup with ROW_NUMBER)
silver_dim_store_df = spark.sql("""
WITH staged AS (
    SELECT
        CAST(TRIM(store_id) AS STRING)                 AS store_id,
        CAST(TRIM(store_name) AS STRING)               AS store_name,
        CAST(TRIM(city) AS STRING)                     AS city,
        CAST(TRIM(state) AS STRING)                    AS state,
        CAST(TRIM(store_type) AS STRING)               AS store_type,
        CAST(open_date AS DATE)                        AS open_date,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(TRIM(store_id) AS STRING)
            ORDER BY CAST(TRIM(store_id) AS STRING) DESC
        ) AS rn
    FROM bronze_stores_raw
    WHERE TRIM(store_id) IS NOT NULL AND TRIM(store_id) <> ''
)
SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    open_date
FROM staged
WHERE rn = 1
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_dim_store.csv")
)

# ===================================================================================
# TABLE: silver.silver_fact_sales_transaction
# Source: bronze.bronze_sales_transactions_raw bstr
# Joins: LEFT JOIN silver_dim_store, silver_dim_product for conformance
# ===================================================================================

# 1) Read source table(s) from S3
bstr_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/bronze_sales_transactions_raw.{FILE_FORMAT}/")
)

# 2) Create temp views (include prior outputs as views for SQL joins)
bstr_df.createOrReplaceTempView("bronze_sales_transactions_raw")
silver_dim_store_df.createOrReplaceTempView("silver_dim_store")
silver_dim_product_df.createOrReplaceTempView("silver_dim_product")

# 3) Transformation SQL (casts, required keys, transaction_date derivation, dedup)
silver_fact_sales_transaction_df = spark.sql("""
WITH staged AS (
    SELECT
        CAST(TRIM(bstr.transaction_id) AS STRING)            AS transaction_id,
        CAST(bstr.transaction_time AS TIMESTAMP)            AS transaction_time,
        CAST(bstr.transaction_time AS DATE)                 AS transaction_date,
        CAST(TRIM(bstr.store_id) AS STRING)                 AS store_id,
        CAST(TRIM(bstr.product_id) AS STRING)               AS product_id,
        CAST(bstr.quantity AS INT)                          AS quantity,
        CAST(bstr.sale_amount AS DECIMAL(38, 10))           AS sale_amount,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(TRIM(bstr.transaction_id) AS STRING)
            ORDER BY CAST(bstr.transaction_time AS TIMESTAMP) DESC
        ) AS rn
    FROM bronze_sales_transactions_raw bstr
    LEFT JOIN silver_dim_store sds
        ON CAST(TRIM(bstr.store_id) AS STRING) = sds.store_id
    LEFT JOIN silver_dim_product sdp
        ON CAST(TRIM(bstr.product_id) AS STRING) = sdp.product_id
    WHERE TRIM(bstr.transaction_id) IS NOT NULL AND TRIM(bstr.transaction_id) <> ''
      AND TRIM(bstr.store_id) IS NOT NULL AND TRIM(bstr.store_id) <> ''
      AND TRIM(bstr.product_id) IS NOT NULL AND TRIM(bstr.product_id) <> ''
)
SELECT
    transaction_id,
    transaction_time,
    transaction_date,
    store_id,
    product_id,
    quantity,
    sale_amount
FROM staged
WHERE rn = 1
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_fact_sales_transaction_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_fact_sales_transaction.csv")
)

job.commit()
```