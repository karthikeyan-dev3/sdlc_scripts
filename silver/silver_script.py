```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended for CSV I/O
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (NO LOOPS; explicit per table)
# -----------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

order_financials_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_financials_bronze.{FILE_FORMAT}/")
)

order_payments_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_payments_bronze.{FILE_FORMAT}/")
)

order_shipping_addresses_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_shipping_addresses_bronze.{FILE_FORMAT}/")
)

order_customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_customers_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
orders_bronze_df.createOrReplaceTempView("orders_bronze")
order_financials_bronze_df.createOrReplaceTempView("order_financials_bronze")
order_payments_bronze_df.createOrReplaceTempView("order_payments_bronze")
order_shipping_addresses_bronze_df.createOrReplaceTempView("order_shipping_addresses_bronze")
order_customers_bronze_df.createOrReplaceTempView("order_customers_bronze")

# ===================================================================================
# TARGET TABLE: silver_orders
# ===================================================================================
silver_orders_sql = """
WITH base AS (
  SELECT
    CAST(ob.transaction_id AS STRING)                                           AS order_id,
    MIN(CAST(ob.transaction_time AS TIMESTAMP)) OVER (PARTITION BY ob.transaction_id) AS order_timestamp,
    CAST(MIN(CAST(ob.transaction_time AS TIMESTAMP)) OVER (PARTITION BY ob.transaction_id) AS DATE) AS order_date,
    CAST(ob.store_id AS STRING)                                                 AS store_id,
    CAST(SUM(COALESCE(CAST(ob.quantity AS INT), 0)) OVER (PARTITION BY ob.transaction_id) AS INT) AS items_count,
    ROW_NUMBER() OVER (
      PARTITION BY ob.transaction_id
      ORDER BY
        CASE WHEN ob.transaction_time IS NULL THEN 1 ELSE 0 END,
        CAST(ob.transaction_time AS TIMESTAMP) ASC
    ) AS rn
  FROM orders_bronze ob
  WHERE ob.transaction_id IS NOT NULL
)
SELECT
  order_id,
  order_timestamp,
  order_date,
  store_id,
  items_count
FROM base
WHERE rn = 1
"""

silver_orders_df = spark.sql(silver_orders_sql)

(
    silver_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_orders.csv")
)

# ===================================================================================
# TARGET TABLE: silver_order_financials
# ===================================================================================
silver_order_financials_sql = """
WITH base AS (
  SELECT
    CAST(ofb.transaction_id AS STRING) AS order_id,
    CAST(SUM(COALESCE(CAST(ofb.sale_amount AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,2))))
         OVER (PARTITION BY ofb.transaction_id) AS DECIMAL(18,2)) AS order_subtotal_amount,
    CAST(0 AS DECIMAL(18,2)) AS order_tax_amount,
    CAST(0 AS DECIMAL(18,2)) AS order_shipping_amount,
    CAST(0 AS DECIMAL(18,2)) AS order_discount_amount,
    CAST(SUM(COALESCE(CAST(ofb.sale_amount AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,2))))
         OVER (PARTITION BY ofb.transaction_id) AS DECIMAL(18,2)) AS order_total_amount,
    CAST('USD' AS STRING) AS currency_code,
    ROW_NUMBER() OVER (
      PARTITION BY ofb.transaction_id
      ORDER BY ofb.transaction_id
    ) AS rn
  FROM order_financials_bronze ofb
  WHERE ofb.transaction_id IS NOT NULL
)
SELECT
  order_id,
  order_subtotal_amount,
  order_tax_amount,
  order_shipping_amount,
  order_discount_amount,
  (order_subtotal_amount + order_tax_amount + order_shipping_amount - order_discount_amount) AS order_total_amount,
  currency_code
FROM base
WHERE rn = 1
"""

silver_order_financials_df = spark.sql(silver_order_financials_sql)

(
    silver_order_financials_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_order_financials.csv")
)

# ===================================================================================
# TARGET TABLE: silver_order_payments
# ===================================================================================
silver_order_payments_sql = """
WITH base AS (
  SELECT
    CAST(opb.transaction_id AS STRING) AS order_id,
    MAX(CAST(opb.transaction_time AS TIMESTAMP)) OVER (PARTITION BY opb.transaction_id) AS payment_timestamp,
    CAST(SUM(COALESCE(CAST(opb.sale_amount AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,2))))
         OVER (PARTITION BY opb.transaction_id) AS DECIMAL(18,2)) AS payment_amount,
    CAST('UNKNOWN' AS STRING) AS payment_method,
    ROW_NUMBER() OVER (
      PARTITION BY opb.transaction_id
      ORDER BY
        CASE WHEN opb.transaction_time IS NULL THEN 1 ELSE 0 END,
        CAST(opb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM order_payments_bronze opb
  WHERE opb.transaction_id IS NOT NULL
)
SELECT
  order_id,
  payment_timestamp,
  payment_amount,
  payment_method
FROM base
WHERE rn = 1
"""

silver_order_payments_df = spark.sql(silver_order_payments_sql)

(
    silver_order_payments_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_order_payments.csv")
)

# ===================================================================================
# TARGET TABLE: silver_order_shipping_addresses
# ===================================================================================
silver_order_shipping_addresses_sql = """
WITH orders_dedup AS (
  SELECT
    CAST(ob.transaction_id AS STRING) AS order_id,
    CAST(ob.store_id AS STRING)       AS store_id,
    ROW_NUMBER() OVER (
      PARTITION BY ob.transaction_id
      ORDER BY
        CASE WHEN ob.transaction_time IS NULL THEN 1 ELSE 0 END,
        CAST(ob.transaction_time AS TIMESTAMP) ASC
    ) AS rn
  FROM orders_bronze ob
  WHERE ob.transaction_id IS NOT NULL
),
orders_distinct AS (
  SELECT
    order_id,
    store_id
  FROM orders_dedup
  WHERE rn = 1
)
SELECT
  od.order_id AS order_id,
  CAST(osab.city AS STRING)  AS shipping_city,
  CAST(osab.state AS STRING) AS shipping_state,
  od.store_id AS store_id,
  CAST('UNKNOWN' AS STRING)  AS shipping_country
FROM orders_distinct od
LEFT JOIN order_shipping_addresses_bronze osab
  ON od.store_id = osab.store_id
"""

silver_order_shipping_addresses_df = spark.sql(silver_order_shipping_addresses_sql)

(
    silver_order_shipping_addresses_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_order_shipping_addresses.csv")
)

# ===================================================================================
# TARGET TABLE: silver_order_customers
# ===================================================================================
silver_order_customers_sql = """
WITH base AS (
  SELECT
    CAST(ocb.transaction_id AS STRING) AS order_id,
    CONCAT('STORE_', COALESCE(CAST(ocb.store_id AS STRING), 'UNKNOWN')) AS customer_id,
    ROW_NUMBER() OVER (
      PARTITION BY ocb.transaction_id
      ORDER BY
        CASE WHEN ocb.transaction_time IS NULL THEN 1 ELSE 0 END,
        CAST(ocb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM order_customers_bronze ocb
  WHERE ocb.transaction_id IS NOT NULL
)
SELECT
  order_id,
  customer_id
FROM base
WHERE rn = 1
"""

silver_order_customers_df = spark.sql(silver_order_customers_sql)

(
    silver_order_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_order_customers.csv")
)

job.commit()
```