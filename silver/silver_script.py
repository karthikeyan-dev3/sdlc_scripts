```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (bronze) and create temp views
# --------------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

orders_data_quality_daily_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_data_quality_daily_bronze.{FILE_FORMAT}/")
)
orders_data_quality_daily_bronze_df.createOrReplaceTempView("orders_data_quality_daily_bronze")

# ======================================================================================
# Target: orders_silver
# - Cleaned and de-duplicated order header at transaction grain
# - De-duplicate on transaction_id keeping latest transaction_time.
# ======================================================================================
orders_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(ob.transaction_id AS STRING)                             AS order_id,
    CAST(ob.transaction_time AS DATE)                             AS order_date,
    CAST(ob.sale_amount AS DECIMAL(38, 10))                       AS order_total_amount,
    CAST(NULL AS STRING)                                          AS customer_id,
    CAST('COMPLETED' AS STRING)                                   AS order_status,
    CAST('USD' AS STRING)                                         AS currency_code,
    CAST('sales_transactions_raw' AS STRING)                      AS source_system,
    CURRENT_DATE                                                  AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY ob.transaction_id
      ORDER BY ob.transaction_time DESC
    ) AS rn
  FROM orders_bronze ob
)
SELECT
  order_id,
  order_date,
  order_total_amount,
  customer_id,
  order_status,
  currency_code,
  source_system,
  ingestion_date
FROM ranked
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

# ======================================================================================
# Target: order_items_silver
# - Cleaned and de-duplicated order line items
# - Filter invalid rows (null/<=0 quantity, null product_id)
# - De-duplicate on (transaction_id, product_id) keeping highest quantity/most recent.
# - order_item_id = ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY product_id)
# ======================================================================================
order_items_silver_sql = """
WITH base AS (
  SELECT
    oib.transaction_id,
    oib.product_id,
    oib.quantity,
    ob.transaction_time,
    ob.sale_amount
  FROM order_items_bronze oib
  INNER JOIN orders_bronze ob
    ON oib.transaction_id = ob.transaction_id
  WHERE oib.product_id IS NOT NULL
    AND oib.quantity IS NOT NULL
    AND oib.quantity > 0
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    quantity,
    transaction_time,
    sale_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id, product_id
      ORDER BY quantity DESC, transaction_time DESC
    ) AS rn_keep
  FROM base
),
kept AS (
  SELECT
    transaction_id,
    product_id,
    quantity,
    sale_amount
  FROM dedup
  WHERE rn_keep = 1
),
final AS (
  SELECT
    CAST(k.transaction_id AS STRING) AS order_id,
    CAST(
      ROW_NUMBER() OVER (
        PARTITION BY k.transaction_id
        ORDER BY k.product_id
      ) AS INT
    ) AS order_item_id,
    CAST(k.product_id AS STRING) AS product_id,
    CAST(k.quantity AS INT) AS quantity,
    CAST(
      CASE
        WHEN k.quantity > 0 THEN k.sale_amount / k.quantity
        ELSE 0
      END AS DECIMAL(38, 10)
    ) AS unit_price,
    CAST(
      CASE
        WHEN k.quantity > 0 THEN (k.sale_amount / k.quantity) * k.quantity
        ELSE 0
      END AS DECIMAL(38, 10)
    ) AS line_amount,
    CAST('USD' AS STRING) AS currency_code,
    CURRENT_DATE AS ingestion_date
  FROM kept k
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount,
  currency_code,
  ingestion_date
FROM final
"""
order_items_silver_df = spark.sql(order_items_silver_sql)

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

# ======================================================================================
# Target: orders_data_quality_daily_silver
# - Daily aggregated DQ metrics
# - run_date=CAST(transaction_time AS DATE), source_system='sales_transactions_raw'
# - latency_minutes per UDT: DATEDIFF(minute, MAX(transaction_time), CURRENT_TIMESTAMP)
# ======================================================================================
orders_data_quality_daily_silver_sql = """
SELECT
  CAST(odqdb.transaction_time AS DATE) AS run_date,
  CAST('sales_transactions_raw' AS STRING) AS source_system,
  CAST(COUNT(*) AS INT) AS total_records,
  CAST(SUM(
    CASE
      WHEN odqdb.transaction_id IS NOT NULL
       AND odqdb.store_id IS NOT NULL
       AND odqdb.sale_amount IS NOT NULL
       AND odqdb.transaction_time IS NOT NULL
       AND odqdb.product_id IS NOT NULL
       AND odqdb.quantity IS NOT NULL
       AND odqdb.quantity > 0
      THEN 1 ELSE 0
    END
  ) AS INT) AS valid_records,
  CAST(
    COUNT(*) - SUM(
      CASE
        WHEN odqdb.transaction_id IS NOT NULL
         AND odqdb.store_id IS NOT NULL
         AND odqdb.sale_amount IS NOT NULL
         AND odqdb.transaction_time IS NOT NULL
         AND odqdb.product_id IS NOT NULL
         AND odqdb.quantity IS NOT NULL
         AND odqdb.quantity > 0
        THEN 1 ELSE 0
      END
    )
  AS INT) AS invalid_records,
  CAST(
    CASE
      WHEN COUNT(*) > 0 THEN
        (
          SUM(
            CASE
              WHEN odqdb.transaction_id IS NOT NULL
               AND odqdb.store_id IS NOT NULL
               AND odqdb.sale_amount IS NOT NULL
               AND odqdb.transaction_time IS NOT NULL
               AND odqdb.product_id IS NOT NULL
               AND odqdb.quantity IS NOT NULL
               AND odqdb.quantity > 0
              THEN 1 ELSE 0
            END
          ) * 100.0
        ) / COUNT(*)
      ELSE 0
    END
  AS DECIMAL(38, 10)) AS accuracy_percent,
  MIN(odqdb.transaction_time) AS load_start_ts,
  MAX(odqdb.transaction_time) AS load_end_ts,
  CAST(DATEDIFF(minute, MAX(odqdb.transaction_time), CURRENT_TIMESTAMP) AS INT) AS latency_minutes
FROM orders_data_quality_daily_bronze odqdb
GROUP BY
  CAST(odqdb.transaction_time AS DATE),
  CAST('sales_transactions_raw' AS STRING)
"""
orders_data_quality_daily_silver_df = spark.sql(orders_data_quality_daily_silver_sql)

(
    orders_data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_data_quality_daily_silver.csv")
)

job.commit()
```