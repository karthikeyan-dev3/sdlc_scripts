```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Context
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

# -----------------------------------------------------------------------------------
# Parameters (fixed as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Source Reads (Bronze)
# NOTE: Must follow strict rule: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
bronze_products_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_products_raw.{FILE_FORMAT}/")
)
bronze_products_raw_df.createOrReplaceTempView("bronze_products_raw")

bronze_stores_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_stores_raw.{FILE_FORMAT}/")
)
bronze_stores_raw_df.createOrReplaceTempView("bronze_stores_raw")

bronze_sales_transactions_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_sales_transactions_raw.{FILE_FORMAT}/")
)
bronze_sales_transactions_raw_df.createOrReplaceTempView("bronze_sales_transactions_raw")

# ===================================================================================
# TARGET TABLE: silver_dim_product
# Source: bronze_products_raw bpr
# Rules: standardize text (TRIM), enforce non-null product_id, dedup to 1 row/product_id
# ===================================================================================
silver_dim_product_sql = """
WITH cleaned AS (
  SELECT
    CAST(TRIM(bpr.product_id) AS STRING)                         AS product_id,
    CAST(TRIM(bpr.product_name) AS STRING)                       AS product_name,
    CAST(TRIM(bpr.category) AS STRING)                           AS category,
    CAST(TRIM(bpr.brand) AS STRING)                              AS brand,
    CAST(bpr.price AS DECIMAL(38,10))                            AS price,
    CAST(bpr.is_active AS BOOLEAN)                               AS is_active
  FROM bronze_products_raw bpr
  WHERE TRIM(COALESCE(bpr.product_id, '')) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN category     IS NOT NULL AND TRIM(category)     <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN brand        IS NOT NULL AND TRIM(brand)        <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN price        IS NOT NULL                              THEN 1 ELSE 0 END DESC,
        CASE WHEN is_active    IS NOT NULL                              THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM cleaned
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM dedup
WHERE rn = 1
"""
silver_dim_product_df = spark.sql(silver_dim_product_sql)
silver_dim_product_df.createOrReplaceTempView("silver_dim_product")

(
    silver_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/silver_dim_product.csv")
)

# ===================================================================================
# TARGET TABLE: silver_dim_store
# Source: bronze_stores_raw bsr
# Rules: standardize text (TRIM), enforce non-null store_id, dedup to 1 row/store_id
# ===================================================================================
silver_dim_store_sql = """
WITH cleaned AS (
  SELECT
    CAST(TRIM(bsr.store_id) AS STRING)                           AS store_id,
    CAST(TRIM(bsr.store_name) AS STRING)                         AS store_name,
    CAST(TRIM(bsr.city) AS STRING)                               AS city,
    CAST(TRIM(bsr.state) AS STRING)                              AS state,
    CAST(TRIM(bsr.store_type) AS STRING)                         AS store_type,
    CAST(bsr.open_date AS DATE)                                  AS open_date
  FROM bronze_stores_raw bsr
  WHERE TRIM(COALESCE(bsr.store_id, '')) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN city       IS NOT NULL AND TRIM(city)       <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN state      IS NOT NULL AND TRIM(state)      <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN store_type IS NOT NULL AND TRIM(store_type) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN open_date  IS NOT NULL                           THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM cleaned
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM dedup
WHERE rn = 1
"""
silver_dim_store_df = spark.sql(silver_dim_store_sql)
silver_dim_store_df.createOrReplaceTempView("silver_dim_store")

(
    silver_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/silver_dim_store.csv")
)

# ===================================================================================
# TARGET TABLE: silver_fact_sales_transaction
# Source: bronze_sales_transactions_raw bstr
# Join: INNER JOIN silver_dim_store sds ON bstr.store_id = sds.store_id
#       INNER JOIN silver_dim_product sdp ON bstr.product_id = sdp.product_id
# Transform: transaction_date = CAST(transaction_time AS DATE)
# Rules: enforce non-null transaction_id/store_id/product_id, dedup using ROW_NUMBER
# ===================================================================================
silver_fact_sales_transaction_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(bstr.transaction_id) AS STRING)                     AS transaction_id,
    CAST(bstr.transaction_time AS TIMESTAMP)                      AS transaction_time,
    CAST(CAST(bstr.transaction_time AS TIMESTAMP) AS DATE)        AS transaction_date,
    CAST(TRIM(bstr.store_id) AS STRING)                           AS store_id,
    CAST(TRIM(bstr.product_id) AS STRING)                         AS product_id,
    CAST(bstr.quantity AS INT)                                    AS quantity,
    CAST(bstr.sale_amount AS DECIMAL(38,10))                      AS sale_amount
  FROM bronze_sales_transactions_raw bstr
  WHERE TRIM(COALESCE(bstr.transaction_id, '')) <> ''
    AND TRIM(COALESCE(bstr.store_id, '')) <> ''
    AND TRIM(COALESCE(bstr.product_id, '')) <> ''
),
conformed AS (
  SELECT
    b.transaction_id,
    b.transaction_time,
    b.transaction_date,
    b.store_id,
    b.product_id,
    b.quantity,
    b.sale_amount
  FROM base b
  INNER JOIN silver_dim_store sds
    ON b.store_id = sds.store_id
  INNER JOIN silver_dim_product sdp
    ON b.product_id = sdp.product_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id, store_id, product_id, transaction_time
      ORDER BY
        CASE WHEN quantity     IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN sale_amount  IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM conformed
)
SELECT
  transaction_id,
  transaction_time,
  transaction_date,
  store_id,
  product_id,
  quantity,
  sale_amount
FROM dedup
WHERE rn = 1
"""
silver_fact_sales_transaction_df = spark.sql(silver_fact_sales_transaction_sql)

(
    silver_fact_sales_transaction_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/silver_fact_sales_transaction.csv")
)
```