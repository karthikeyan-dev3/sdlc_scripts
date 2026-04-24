```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# --------------------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------------------
# Read sources (S3) + Temp Views
# NOTE: Source reading rule (STRICT): .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------------------------
df_bronze_customer_orders = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_bronze_customer_orders.createOrReplaceTempView("customer_orders")

df_bronze_customer_order_items = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_bronze_customer_order_items.createOrReplaceTempView("customer_order_items")

df_bronze_order_data_quality_daily = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_data_quality_daily.{FILE_FORMAT}/")
)
df_bronze_order_data_quality_daily.createOrReplaceTempView("order_data_quality_daily")

# ==================================================================================================
# TARGET TABLE: silver.customer_orders
# - De-duplicates to one record per transaction_id (kept latest by transaction_time)
# - order_id = co.transaction_id
# - order_date = CAST(co.transaction_time AS DATE)
# - order_timestamp = co.transaction_time
# - customer_id = co.store_id
# - sale_amount = co.sale_amount
# - subtotal_amount/total_amount derived from sale_amount
# - item_count from summed line quantities joined from bronze.customer_order_items
# - Conformed defaults: currency_code, order_status, source_system, ingestion_date
# ==================================================================================================
sql_customer_orders = """
WITH co_dedup AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.transaction_id
      ORDER BY co.transaction_time DESC
    ) AS rn
  FROM customer_orders co
),
co_latest AS (
  SELECT
    co.transaction_id,
    co.transaction_time,
    co.store_id,
    co.sale_amount
  FROM co_dedup co
  WHERE co.rn = 1
),
item_qty AS (
  SELECT
    coi.transaction_id,
    SUM(CAST(coi.quantity AS INT)) AS item_count
  FROM customer_order_items coi
  GROUP BY coi.transaction_id
)
SELECT
  -- UDT exact transformations
  CAST(co.transaction_id AS STRING)                         AS order_id,
  CAST(co.transaction_time AS DATE)                         AS order_date,
  co.transaction_time                                       AS order_timestamp,
  CAST(co.store_id AS STRING)                               AS customer_id,
  CAST(co.sale_amount AS DECIMAL(18,2))                     AS sale_amount,

  -- Derived (per description)
  CAST(co.sale_amount AS DECIMAL(18,2))                     AS subtotal_amount,
  CAST(co.sale_amount AS DECIMAL(18,2))                     AS total_amount,
  COALESCE(iq.item_count, 0)                                AS item_count,

  -- Conformed defaults
  UPPER(TRIM(COALESCE(NULL, 'USD')))                        AS currency_code,
  UPPER(TRIM(COALESCE(NULL, 'COMPLETED')))                  AS order_status,
  LOWER(TRIM(COALESCE(NULL, 'bronze')))                     AS source_system,
  CAST(current_date() AS DATE)                              AS ingestion_date
FROM co_latest co
LEFT JOIN item_qty iq
  ON iq.transaction_id = co.transaction_id
"""

df_silver_customer_orders = spark.sql(sql_customer_orders)

df_silver_customer_orders.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_orders.csv"
)

# ==================================================================================================
# TARGET TABLE: silver.customer_order_items
# - Ensures referential integrity by join to orders
# - De-duplicates on (transaction_id, product_id) by aggregating quantity
# - order_id = coi.transaction_id
# - product_id = coi.product_id
# - quantity = summed
# - order_item_id deterministically per (order_id, product_id)
# - currency_code conformed, unit_price/line_total_amount derived where possible from header sale_amount
# - ingestion_date
# ==================================================================================================
sql_customer_order_items = """
WITH co_dedup AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.transaction_id
      ORDER BY co.transaction_time DESC
    ) AS rn
  FROM customer_orders co
),
co_latest AS (
  SELECT
    co.transaction_id,
    co.sale_amount
  FROM co_dedup co
  WHERE co.rn = 1
),
coi_agg AS (
  SELECT
    coi.transaction_id,
    coi.product_id,
    SUM(CAST(coi.quantity AS INT)) AS quantity
  FROM customer_order_items coi
  GROUP BY coi.transaction_id, coi.product_id
),
order_tot_qty AS (
  SELECT
    transaction_id,
    SUM(quantity) AS total_quantity
  FROM coi_agg
  GROUP BY transaction_id
)
SELECT
  -- UDT exact transformations
  CAST(coi.transaction_id AS STRING)                        AS order_id,
  CAST(coi.product_id AS STRING)                            AS product_id,
  CAST(coi.quantity AS INT)                                 AS quantity,

  -- Deterministic ID per (order_id, product_id)
  CAST(concat(CAST(coi.transaction_id AS STRING), '-', CAST(coi.product_id AS STRING)) AS STRING)
                                                           AS order_item_id,

  -- Conformed defaults
  UPPER(TRIM(COALESCE(NULL, 'USD')))                        AS currency_code,

  -- Derived pricing (best-effort from header sale_amount with proportional allocation by quantity)
  CAST(
    CASE
      WHEN COALESCE(otq.total_quantity, 0) > 0 THEN (CAST(co.sale_amount AS DECIMAL(18,2)) / CAST(otq.total_quantity AS DECIMAL(18,2)))
      ELSE CAST(NULL AS DECIMAL(18,2))
    END
    AS DECIMAL(18,2)
  )                                                         AS unit_price,
  CAST(
    CASE
      WHEN COALESCE(otq.total_quantity, 0) > 0 THEN (CAST(co.sale_amount AS DECIMAL(18,2)) * (CAST(coi.quantity AS DECIMAL(18,2)) / CAST(otq.total_quantity AS DECIMAL(18,2))))
      ELSE CAST(NULL AS DECIMAL(18,2))
    END
    AS DECIMAL(18,2)
  )                                                         AS line_total_amount,

  CAST(current_date() AS DATE)                              AS ingestion_date
FROM coi_agg coi
INNER JOIN co_latest co
  ON co.transaction_id = coi.transaction_id
LEFT JOIN order_tot_qty otq
  ON otq.transaction_id = coi.transaction_id
"""

df_silver_customer_order_items = spark.sql(sql_customer_order_items)

df_silver_customer_order_items.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_order_items.csv"
)

# ==================================================================================================
# TARGET TABLE: silver.order_data_quality_daily
# - De-duplicates raw records
# - run_date = odqd.transaction_date
# - source_system defaulted
# ==================================================================================================
sql_order_data_quality_daily = """
WITH odqd_dedup AS (
  SELECT
    odqd.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        odqd.transaction_date,
        odqd.transaction_id,
        odqd.store_id,
        odqd.product_id,
        odqd.quantity,
        odqd.sale_amount,
        odqd.transaction_time
      ORDER BY odqd.transaction_time DESC
    ) AS rn
  FROM order_data_quality_daily odqd
)
SELECT
  -- UDT exact transformations
  CAST(odqd.transaction_date AS DATE)                       AS run_date,
  CAST(odqd.transaction_id AS STRING)                       AS transaction_id,
  CAST(odqd.store_id AS STRING)                             AS store_id,
  CAST(odqd.product_id AS STRING)                           AS product_id,
  CAST(odqd.quantity AS INT)                                AS quantity,
  CAST(odqd.sale_amount AS DECIMAL(18,2))                   AS sale_amount,
  odqd.transaction_time                                     AS transaction_time,

  -- Conformed default
  LOWER(TRIM(COALESCE(NULL, 'bronze')))                     AS source_system,
  CAST(current_date() AS DATE)                              AS ingestion_date
FROM odqd_dedup odqd
WHERE odqd.rn = 1
"""

df_silver_order_data_quality_daily = spark.sql(sql_order_data_quality_daily)

df_silver_order_data_quality_daily.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/order_data_quality_daily.csv"
)

job.commit()
```