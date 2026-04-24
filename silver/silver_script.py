```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Ingestion/runtime context (used where UDT says "from load context")
# (kept as deterministic values at runtime; can be overridden via job params if desired)
INGESTION_DATE = spark.sql("SELECT current_date() AS d").first()["d"]
SOURCE_FILE_FORMAT = FILE_FORMAT.upper()

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

# ------------------------------------------------------------------------------------
# 2) TARGET: silver.orders_silver
#    - one deduplicated order per transaction_id
#    - os.order_id = ob.transaction_id
#    - os.order_date = DATE(ob.transaction_time)
#    - os.customer_id = ob.store_id
#    - os.order_total_amount = SUM(ob.sale_amount) per transaction_id
#    - currency_code default 'USD' if not provided
#    - order_status default 'completed' if not provided
#    - shipping fields NULL (not available)
#    - include ingestion/source metadata from load context
# ------------------------------------------------------------------------------------
orders_silver_sql = f"""
WITH base AS (
  SELECT
    TRIM(CAST(ob.transaction_id AS STRING))                         AS order_id,
    DATE(ob.transaction_time)                                       AS order_date,
    TRIM(CAST(ob.store_id AS STRING))                               AS customer_id,
    CAST(ob.sale_amount AS DECIMAL(38, 10))                         AS sale_amount,
    ob.currency_code                                                AS currency_code_raw,
    ob.order_status                                                 AS order_status_raw,
    ob.transaction_time                                             AS transaction_time
  FROM orders_bronze ob
  WHERE ob.transaction_id IS NOT NULL
),
agg AS (
  SELECT
    order_id,
    order_date,
    customer_id,
    CAST(SUM(COALESCE(sale_amount, CAST(0 AS DECIMAL(38, 10)))) AS DECIMAL(38, 10)) AS order_total_amount,
    COALESCE(TRIM(CAST(MAX(currency_code_raw) AS STRING)), 'USD')                   AS currency_code,
    COALESCE(TRIM(CAST(MAX(order_status_raw) AS STRING)), 'completed')              AS order_status,
    MAX(transaction_time)                                                          AS max_transaction_time
  FROM base
  GROUP BY order_id, order_date, customer_id
),
dedup AS (
  SELECT
    a.*,
    ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY a.max_transaction_time DESC) AS rn
  FROM agg a
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_total_amount,
  currency_code,
  order_status,
  CAST(NULL AS STRING) AS shipping_country,
  CAST(NULL AS STRING) AS shipping_state,
  CAST(NULL AS STRING) AS shipping_city,
  DATE('{INGESTION_DATE}') AS ingestion_date,
  '{SOURCE_FILE_FORMAT}' AS source_file_format
FROM dedup
WHERE rn = 1
"""
orders_silver_df = spark.sql(orders_silver_sql)

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

# Create view for downstream silver tables
orders_silver_df.createOrReplaceTempView("orders_silver")

# ------------------------------------------------------------------------------------
# 3) TARGET: silver.customers_silver
#    - one deduplicated customer per store_id from bronze.orders_bronze
#    - cs.customer_id = ob.store_id
#    - customer_name NULL
#    - customer_email_masked NULL
# ------------------------------------------------------------------------------------
customers_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(ob.store_id AS STRING)) AS customer_id,
    ob.transaction_time              AS transaction_time
  FROM orders_bronze ob
  WHERE ob.store_id IS NOT NULL
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.customer_id ORDER BY b.transaction_time DESC) AS rn
  FROM base b
)
SELECT
  customer_id,
  CAST(NULL AS STRING) AS customer_name,
  CAST(NULL AS STRING) AS customer_email_masked
FROM dedup
WHERE rn = 1
"""
customers_silver_df = spark.sql(customers_silver_sql)

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

# ------------------------------------------------------------------------------------
# 4) TARGET: silver.payments_silver
#    - one deduplicated payment per transaction_id from bronze.orders_bronze
#    - ps.order_id = ob.transaction_id
#    - ps.amount = SUM(ob.sale_amount) per transaction_id
#    - currency_code default 'USD' if not provided
#    - payment_method NULL
# ------------------------------------------------------------------------------------
payments_silver_sql = f"""
WITH base AS (
  SELECT
    TRIM(CAST(ob.transaction_id AS STRING))                 AS order_id,
    CAST(ob.sale_amount AS DECIMAL(38, 10))                 AS sale_amount,
    ob.currency_code                                        AS currency_code_raw,
    ob.transaction_time                                     AS transaction_time
  FROM orders_bronze ob
  WHERE ob.transaction_id IS NOT NULL
),
agg AS (
  SELECT
    order_id,
    CAST(SUM(COALESCE(sale_amount, CAST(0 AS DECIMAL(38, 10)))) AS DECIMAL(38, 10)) AS amount,
    COALESCE(TRIM(CAST(MAX(currency_code_raw) AS STRING)), 'USD')                   AS currency_code,
    MAX(transaction_time)                                                          AS max_transaction_time
  FROM base
  GROUP BY order_id
),
dedup AS (
  SELECT
    a.*,
    ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY a.max_transaction_time DESC) AS rn
  FROM agg a
)
SELECT
  order_id,
  CAST(NULL AS STRING) AS payment_method,
  amount,
  currency_code,
  DATE('{INGESTION_DATE}') AS ingestion_date
FROM dedup
WHERE rn = 1
"""
payments_silver_df = spark.sql(payments_silver_sql)

(
    payments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/payments_silver.csv")
)

# ------------------------------------------------------------------------------------
# 5) TARGET: silver.shipments_silver
#    - one deduplicated shipment per transaction_id from bronze.orders_bronze
#    - ss.order_id = ob.transaction_id
#    - shipping_* NULL (not available)
# ------------------------------------------------------------------------------------
shipments_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(ob.transaction_id AS STRING)) AS order_id,
    ob.transaction_time                     AS transaction_time
  FROM orders_bronze ob
  WHERE ob.transaction_id IS NOT NULL
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY b.transaction_time DESC) AS rn
  FROM base b
)
SELECT
  order_id,
  CAST(NULL AS STRING) AS shipping_country,
  CAST(NULL AS STRING) AS shipping_state,
  CAST(NULL AS STRING) AS shipping_city
FROM dedup
WHERE rn = 1
"""
shipments_silver_df = spark.sql(shipments_silver_sql)

(
    shipments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/shipments_silver.csv")
)

# ------------------------------------------------------------------------------------
# 6) TARGET: silver.order_status_silver
#    - from silver.orders_silver
#    - oss.order_id = os.order_id
#    - oss.order_status = COALESCE(os.order_status,'completed')
# ------------------------------------------------------------------------------------
order_status_silver_sql = """
WITH base AS (
  SELECT
    TRIM(CAST(os.order_id AS STRING)) AS order_id,
    COALESCE(TRIM(CAST(os.order_status AS STRING)), 'completed') AS order_status
  FROM orders_silver os
  WHERE os.order_id IS NOT NULL
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY b.order_id) AS rn
  FROM base b
)
SELECT
  order_id,
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

# ------------------------------------------------------------------------------------
# 7) TARGET: silver.currencies_silver
#    - from silver.orders_silver
#    - cur.currency_code = COALESCE(os.currency_code,'USD')
#    - one row per currency_code
# ------------------------------------------------------------------------------------
currencies_silver_sql = """
WITH base AS (
  SELECT
    COALESCE(TRIM(CAST(os.currency_code AS STRING)), 'USD') AS currency_code
  FROM orders_silver os
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.currency_code ORDER BY b.currency_code) AS rn
  FROM base b
)
SELECT
  currency_code
FROM dedup
WHERE rn = 1
"""
currencies_silver_df = spark.sql(currencies_silver_sql)

(
    currencies_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/currencies_silver.csv")
)

# ------------------------------------------------------------------------------------
# 8) TARGET: silver.source_files_silver
#    - per-load source file metadata (from runtime/context)
#    - one record per load batch
# ------------------------------------------------------------------------------------
source_files_silver_sql = f"""
SELECT
  '{SOURCE_FILE_FORMAT}' AS source_file_format,
  DATE('{INGESTION_DATE}') AS ingestion_date
"""
source_files_silver_df = spark.sql(source_files_silver_sql)

(
    source_files_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/source_files_silver.csv")
)

# ------------------------------------------------------------------------------------
# 9) TARGET: silver.ingestions_silver
#    - per-load ingestion metadata (from runtime/context)
#    - one record per load batch
# ------------------------------------------------------------------------------------
ingestions_silver_sql = f"""
SELECT
  DATE('{INGESTION_DATE}') AS ingestion_date
"""
ingestions_silver_df = spark.sql(ingestions_silver_sql)

(
    ingestions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/ingestions_silver.csv")
)

job.commit()
```