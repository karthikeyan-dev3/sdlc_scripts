```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------------------
# AWS Glue Boilerplate
# --------------------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended options for CSV handling
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# --------------------------------------------------------------------------------------------------
# 1) SOURCE READS (Bronze) + TEMP VIEWS
# --------------------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# --------------------------------------------------------------------------------------------------
# 2) TARGET: stores_silver (dedup + standardization)
#    - enforce non-null store_id
#    - TRIM/UPPER store_name, city, store_type
#    - ROW_NUMBER de-dup (best-effort ordering using open_date then ingestion_ts if present)
# --------------------------------------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING)                                         AS store_id,
    CAST(UPPER(TRIM(sb.store_name)) AS STRING)                                AS store_name,
    CAST(UPPER(TRIM(sb.city)) AS STRING)                                      AS city,
    CAST(UPPER(TRIM(sb.store_type)) AS STRING)                                AS store_type,

    -- For dedup ordering (best-effort with optional columns if present in source):
    -- Using COALESCE so query remains valid even if values are null (columns must exist to be used).
    CAST(sb.open_date AS TIMESTAMP)                                           AS open_date_ts,
    CAST(sb.ingestion_ts AS TIMESTAMP)                                        AS ingestion_ts
  FROM stores_bronze sb
  WHERE TRIM(COALESCE(sb.store_id, '')) <> ''
),
dedup AS (
  SELECT
    store_id, store_name, city, store_type,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date_ts DESC, ingestion_ts DESC
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
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# --------------------------------------------------------------------------------------------------
# 3) TARGET: products_silver (dedup + standardization)
#    - enforce non-null product_id
#    - TRIM/UPPER product_name, category, brand
#    - keep is_active
#    - ROW_NUMBER de-dup (best-effort ordering using updated_at then ingestion_ts if present)
# --------------------------------------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)                                       AS product_id,
    CAST(UPPER(TRIM(pb.product_name)) AS STRING)                              AS product_name,
    CAST(UPPER(TRIM(pb.category)) AS STRING)                                  AS category,
    CAST(UPPER(TRIM(pb.brand)) AS STRING)                                     AS brand,
    CAST(pb.is_active AS BOOLEAN)                                             AS is_active,

    -- For dedup ordering (best-effort with optional columns if present in source)
    CAST(pb.updated_at AS TIMESTAMP)                                          AS updated_at_ts,
    CAST(pb.ingestion_ts AS TIMESTAMP)                                        AS ingestion_ts
  FROM products_bronze pb
  WHERE TRIM(COALESCE(pb.product_id, '')) <> ''
),
dedup AS (
  SELECT
    product_id, product_name, category, brand, is_active,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY updated_at_ts DESC, ingestion_ts DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  is_active
FROM dedup
WHERE rn = 1
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

# --------------------------------------------------------------------------------------------------
# 4) TARGET: transactions_silver (dedup + type cleanup + derived sales_date)
#    - enforce non-null transaction_id/store_id/product_id
#    - CAST quantity (INT), sale_amount (DECIMAL)
#    - handle negative/null: quantity >= 0, sale_amount >= 0 (set to 0 when negative, keep nulls as 0)
#    - normalize transaction_time (cast to TIMESTAMP)
#    - derive sales_date = DATE(transaction_time)
#    - ROW_NUMBER de-dup by transaction_id order by transaction_time desc
# --------------------------------------------------------------------------------------------------
transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(tb.transaction_id) AS STRING)                                   AS transaction_id,
    CAST(TRIM(tb.store_id) AS STRING)                                         AS store_id,
    CAST(TRIM(tb.product_id) AS STRING)                                       AS product_id,

    CASE
      WHEN CAST(tb.quantity AS INT) IS NULL THEN 0
      WHEN CAST(tb.quantity AS INT) < 0 THEN 0
      ELSE CAST(tb.quantity AS INT)
    END                                                                       AS quantity,

    CASE
      WHEN CAST(tb.sale_amount AS DECIMAL(18,2)) IS NULL THEN CAST(0.00 AS DECIMAL(18,2))
      WHEN CAST(tb.sale_amount AS DECIMAL(18,2)) < CAST(0.00 AS DECIMAL(18,2)) THEN CAST(0.00 AS DECIMAL(18,2))
      ELSE CAST(tb.sale_amount AS DECIMAL(18,2))
    END                                                                       AS sale_amount,

    CAST(tb.transaction_time AS TIMESTAMP)                                    AS transaction_time
  FROM transactions_bronze tb
  WHERE TRIM(COALESCE(tb.transaction_id, '')) <> ''
    AND TRIM(COALESCE(tb.store_id, '')) <> ''
    AND TRIM(COALESCE(tb.product_id, '')) <> ''
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time,
  DATE(transaction_time) AS sales_date
FROM dedup
WHERE rn = 1
"""
transactions_silver_df = spark.sql(transactions_silver_sql)
transactions_silver_df.createOrReplaceTempView("transactions_silver")

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# --------------------------------------------------------------------------------------------------
# 5) TARGET: sales_enriched_silver (join conformed dims + facts)
#    mapping_details:
#      silver.transactions_silver ts
#      INNER JOIN silver.stores_silver ss ON ts.store_id = ss.store_id
#      INNER JOIN silver.products_silver ps ON ts.product_id = ps.product_id
#    Output columns per UDT:
#      sales_date, store_id, store_name, city, store_type, product_id, product_name, category, brand,
#      quantity, sale_amount, transaction_id
# --------------------------------------------------------------------------------------------------
sales_enriched_silver_sql = """
SELECT
  ts.sales_date                                                            AS sales_date,
  ts.store_id                                                              AS store_id,
  ss.store_name                                                            AS store_name,
  ss.city                                                                  AS city,
  ss.store_type                                                            AS store_type,
  ts.product_id                                                            AS product_id,
  ps.product_name                                                          AS product_name,
  ps.category                                                              AS category,
  ps.brand                                                                 AS brand,
  ts.quantity                                                              AS quantity,
  ts.sale_amount                                                           AS sale_amount,
  ts.transaction_id                                                        AS transaction_id
FROM transactions_silver ts
INNER JOIN stores_silver ss
  ON ts.store_id = ss.store_id
INNER JOIN products_silver ps
  ON ts.product_id = ps.product_id
"""
sales_enriched_silver_df = spark.sql(sales_enriched_silver_sql)

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

job.commit()
```