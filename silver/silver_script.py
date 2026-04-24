```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark setup
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Common CSV options (adjust if your bronze has different settings)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
}

# --------------------------------------------------------------------------------------
# 1) customers_silver
#    Source: customers_raw (S3)
# --------------------------------------------------------------------------------------
customers_raw_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customers_raw.{FILE_FORMAT}/")
)
customers_raw_df.createOrReplaceTempView("customers_raw")

customers_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(TRIM(c.customer_id) AS STRING)                                         AS customer_id,
    CAST(NULLIF(TRIM(c.customer_name), '') AS STRING)                           AS customer_name,
    CAST(LOWER(NULLIF(TRIM(c.customer_email), '')) AS STRING)                   AS customer_email,
    CAST(NULLIF(TRIM(c.customer_phone), '') AS STRING)                          AS customer_phone,
    CAST(c.record_created_ts AS TIMESTAMP)                                      AS record_created_ts,
    CAST(c.record_updated_ts AS TIMESTAMP)                                      AS record_updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(c.customer_id) AS STRING)
      ORDER BY CAST(c.record_updated_ts AS TIMESTAMP) DESC, CAST(c.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM customers_raw c
)
SELECT
  customer_id,
  customer_name,
  customer_email,
  customer_phone,
  record_created_ts,
  record_updated_ts
FROM base
WHERE rn = 1
""")
customers_silver_df.createOrReplaceTempView("customers_silver")

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

# --------------------------------------------------------------------------------------
# 2) shipping_addresses_silver
#    Source: shipping_addresses_raw (S3)
# --------------------------------------------------------------------------------------
shipping_addresses_raw_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/shipping_addresses_raw.{FILE_FORMAT}/")
)
shipping_addresses_raw_df.createOrReplaceTempView("shipping_addresses_raw")

shipping_addresses_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(TRIM(sa.shipping_address_id) AS STRING)                                AS shipping_address_id,
    CAST(NULLIF(TRIM(sa.address_line1), '') AS STRING)                          AS address_line1,
    CAST(NULLIF(TRIM(sa.address_line2), '') AS STRING)                          AS address_line2,
    CAST(NULLIF(TRIM(sa.city), '') AS STRING)                                   AS city,
    CAST(UPPER(NULLIF(TRIM(sa.state), '')) AS STRING)                           AS state,
    CAST(NULLIF(TRIM(sa.postal_code), '') AS STRING)                            AS postal_code,
    CAST(UPPER(NULLIF(TRIM(sa.country), '')) AS STRING)                         AS country,
    CAST(sa.record_created_ts AS TIMESTAMP)                                     AS record_created_ts,
    CAST(sa.record_updated_ts AS TIMESTAMP)                                     AS record_updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(sa.shipping_address_id) AS STRING)
      ORDER BY CAST(sa.record_updated_ts AS TIMESTAMP) DESC, CAST(sa.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM shipping_addresses_raw sa
)
SELECT
  shipping_address_id,
  address_line1,
  address_line2,
  city,
  state,
  postal_code,
  country,
  record_created_ts,
  record_updated_ts
FROM base
WHERE rn = 1
""")
shipping_addresses_silver_df.createOrReplaceTempView("shipping_addresses_silver")

(
    shipping_addresses_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/shipping_addresses_silver.csv")
)

# --------------------------------------------------------------------------------------
# 3) products_silver
#    Source: products_bronze (S3)  [UDT mapping_details: bronze.products_bronze pb]
# --------------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)                                         AS product_id,
    CAST(NULLIF(TRIM(pb.product_name), '') AS STRING)                           AS product_name,
    CAST(NULLIF(TRIM(pb.category), '') AS STRING)                               AS product_category,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
      ORDER BY CAST(pb.product_id AS STRING) DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  product_category
FROM base
WHERE rn = 1
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# --------------------------------------------------------------------------------------
# 4) payments_silver
#    Source: payments_raw (S3)
# --------------------------------------------------------------------------------------
payments_raw_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/payments_raw.{FILE_FORMAT}/")
)
payments_raw_df.createOrReplaceTempView("payments_raw")

payments_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(TRIM(pay.order_id) AS STRING)                                          AS order_id,
    CAST(LOWER(NULLIF(TRIM(pay.payment_method), '')) AS STRING)                 AS payment_method,
    CAST(UPPER(NULLIF(TRIM(pay.currency_code), '')) AS STRING)                  AS currency_code,
    CAST(pay.record_created_ts AS TIMESTAMP)                                    AS record_created_ts,
    CAST(pay.record_updated_ts AS TIMESTAMP)                                    AS record_updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pay.order_id) AS STRING)
      ORDER BY CAST(pay.record_updated_ts AS TIMESTAMP) DESC, CAST(pay.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM payments_raw pay
)
SELECT
  order_id,
  payment_method,
  currency_code,
  record_created_ts,
  record_updated_ts
FROM base
WHERE rn = 1
""")
payments_silver_df.createOrReplaceTempView("payments_silver")

(
    payments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/payments_silver.csv")
)

# --------------------------------------------------------------------------------------
# 5) orders_silver
#    Source: orders_raw (S3) + joins to customers_silver, shipping_addresses_silver, payments_silver
# --------------------------------------------------------------------------------------
orders_raw_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/orders_raw.{FILE_FORMAT}/")
)
orders_raw_df.createOrReplaceTempView("orders_raw")

orders_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    CAST(TRIM(o.order_id) AS STRING)                                            AS order_id,
    CAST(DATE(o.order_date) AS DATE)                                            AS order_date,
    CAST(TRIM(o.customer_id) AS STRING)                                         AS customer_id,

    CAST(cs.customer_name AS STRING)                                            AS customer_name,
    CAST(cs.customer_email AS STRING)                                           AS customer_email,
    CAST(cs.customer_phone AS STRING)                                           AS customer_phone,

    CAST(sas.address_line1 AS STRING)                                           AS shipping_address_line1,
    CAST(sas.address_line2 AS STRING)                                           AS shipping_address_line2,
    CAST(sas.city AS STRING)                                                    AS shipping_city,
    CAST(sas.state AS STRING)                                                   AS shipping_state,
    CAST(sas.postal_code AS STRING)                                             AS shipping_postal_code,
    CAST(sas.country AS STRING)                                                 AS shipping_country,

    CAST(UPPER(NULLIF(TRIM(o.order_status), '')) AS STRING)                     AS order_status,
    CAST(o.order_total_amount AS DECIMAL(38, 10))                               AS order_total_amount,

    CAST(pays.currency_code AS STRING)                                          AS currency_code,
    CAST(pays.payment_method AS STRING)                                         AS payment_method,

    CAST(NULLIF(TRIM(o.source_system), '') AS STRING)                           AS source_system,
    CAST(DATE(o.ingestion_date) AS DATE)                                        AS ingestion_date,
    CAST(o.record_created_ts AS TIMESTAMP)                                      AS record_created_ts,
    CAST(o.record_updated_ts AS TIMESTAMP)                                      AS record_updated_ts,

    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(o.order_id) AS STRING)
      ORDER BY CAST(o.record_updated_ts AS TIMESTAMP) DESC, CAST(o.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM orders_raw o
  LEFT JOIN customers_silver cs
    ON CAST(TRIM(o.customer_id) AS STRING) = CAST(TRIM(cs.customer_id) AS STRING)
  LEFT JOIN shipping_addresses_silver sas
    ON CAST(TRIM(o.shipping_address_id) AS STRING) = CAST(TRIM(sas.shipping_address_id) AS STRING)
  LEFT JOIN payments_silver pays
    ON CAST(TRIM(o.order_id) AS STRING) = CAST(TRIM(pays.order_id) AS STRING)
)
SELECT
  order_id,
  order_date,
  customer_id,
  customer_name,
  customer_email,
  customer_phone,
  shipping_address_line1,
  shipping_address_line2,
  shipping_city,
  shipping_state,
  shipping_postal_code,
  shipping_country,
  order_status,
  order_total_amount,
  currency_code,
  payment_method,
  source_system,
  ingestion_date,
  record_created_ts,
  record_updated_ts
FROM joined
WHERE rn = 1
""")
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

# --------------------------------------------------------------------------------------
# 6) order_items_silver
#    Source: order_items_raw (S3) + joins to orders_silver, products_silver
# --------------------------------------------------------------------------------------
order_items_raw_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/order_items_raw.{FILE_FORMAT}/")
)
order_items_raw_df.createOrReplaceTempView("order_items_raw")

order_items_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    CAST(TRIM(oi.order_id) AS STRING)                                           AS order_id,
    CAST(TRIM(oi.order_item_id) AS STRING)                                      AS order_item_id,
    CAST(TRIM(oi.product_id) AS STRING)                                         AS product_id,

    CAST(ps.product_name AS STRING)                                             AS product_name,
    CAST(ps.product_category AS STRING)                                         AS product_category,

    CAST(oi.quantity AS INT)                                                    AS quantity,
    CAST(oi.unit_price AS DECIMAL(38, 10))                                      AS unit_price,
    CAST(oi.item_discount_amount AS DECIMAL(38, 10))                            AS item_discount_amount,
    CAST(oi.item_tax_amount AS DECIMAL(38, 10))                                 AS item_tax_amount,
    CAST(oi.item_total_amount AS DECIMAL(38, 10))                               AS item_total_amount,

    CAST(os.currency_code AS STRING)                                            AS currency_code,
    CAST(DATE(os.ingestion_date) AS DATE)                                       AS ingestion_date,

    CAST(oi.record_created_ts AS TIMESTAMP)                                     AS record_created_ts,
    CAST(oi.record_updated_ts AS TIMESTAMP)                                     AS record_updated_ts,

    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(oi.order_id) AS STRING), CAST(TRIM(oi.order_item_id) AS STRING)
      ORDER BY CAST(oi.record_updated_ts AS TIMESTAMP) DESC, CAST(oi.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM order_items_raw oi
  INNER JOIN orders_silver os
    ON CAST(TRIM(oi.order_id) AS STRING) = CAST(TRIM(os.order_id) AS STRING)
  LEFT JOIN products_silver ps
    ON CAST(TRIM(oi.product_id) AS STRING) = CAST(TRIM(ps.product_id) AS STRING)
)
SELECT
  order_id,
  order_item_id,
  product_id,
  product_name,
  product_category,
  quantity,
  unit_price,
  item_discount_amount,
  item_tax_amount,
  item_total_amount,
  currency_code,
  ingestion_date,
  record_created_ts,
  record_updated_ts
FROM joined
WHERE rn = 1
""")
order_items_silver_df.createOrReplaceTempView("order_items_silver")

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

job.commit()
```