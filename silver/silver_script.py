```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source reads (S3) + temp views
# NOTE: Per requirement, path format is: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

etl_order_data_quality_daily_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_order_data_quality_daily_bronze.{FILE_FORMAT}/")
)
etl_order_data_quality_daily_bronze_df.createOrReplaceTempView("etl_order_data_quality_daily_bronze")

# ====================================================================================
# TARGET TABLE: customer_orders_silver
# ====================================================================================
customer_orders_silver_sql = """
WITH ranked AS (
  SELECT
    -- cos.order_id = cob.transaction_id
    CAST(cob.transaction_id AS STRING) AS order_id,

    -- cos.order_date = CAST(cob.transaction_time AS DATE)
    CAST(cob.transaction_time AS DATE) AS order_date,

    -- cos.customer_id = cob.store_id
    CAST(cob.store_id AS STRING) AS customer_id,

    -- cos.order_status = CASE WHEN cob.transaction_id IS NOT NULL THEN 'COMPLETED' END
    CASE WHEN cob.transaction_id IS NOT NULL THEN 'COMPLETED' END AS order_status,

    -- cos.order_total_amount = cob.sale_amount
    CAST(cob.sale_amount AS DECIMAL(18,2)) AS order_total_amount,

    -- cos.currency_code = 'USD'
    'USD' AS currency_code,

    -- cos.source_system = 'sales_transactions_raw'
    'sales_transactions_raw' AS source_system,

    -- cos.ingestion_date = CAST(cob.transaction_time AS DATE)
    CAST(cob.transaction_time AS DATE) AS ingestion_date,

    -- cos.loaded_at = CURRENT_TIMESTAMP
    CURRENT_TIMESTAMP AS loaded_at,

    ROW_NUMBER() OVER (
      PARTITION BY cob.transaction_id
      ORDER BY cob.transaction_time DESC
    ) AS rn
  FROM customer_orders_bronze cob
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_total_amount,
  currency_code,
  source_system,
  ingestion_date,
  loaded_at
FROM ranked
WHERE rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ====================================================================================
# TARGET TABLE: customer_order_items_silver
# ====================================================================================
customer_order_items_silver_sql = """
WITH filtered AS (
  SELECT
    *
  FROM customer_order_items_bronze coib
  WHERE
    coib.transaction_id IS NOT NULL
    AND coib.product_id IS NOT NULL
    AND coib.quantity IS NOT NULL
    AND CAST(coib.quantity AS INT) > 0
    AND coib.sale_amount IS NOT NULL
),
ranked AS (
  SELECT
    -- cois.order_id = coib.transaction_id
    CAST(coib.transaction_id AS STRING) AS order_id,

    -- cois.order_item_id = md5(coib.transaction_id||'|'||coib.product_id||'|'||COALESCE(CAST(coib.quantity AS VARCHAR),'')||'|'||COALESCE(CAST(coib.transaction_time AS VARCHAR),''))
    md5(
      concat(
        CAST(coib.transaction_id AS STRING), '|',
        CAST(coib.product_id AS STRING), '|',
        COALESCE(CAST(coib.quantity AS STRING), ''), '|',
        COALESCE(CAST(coib.transaction_time AS STRING), '')
      )
    ) AS order_item_id,

    -- cois.product_id = coib.product_id
    CAST(coib.product_id AS STRING) AS product_id,

    -- cois.quantity = coib.quantity
    CAST(coib.quantity AS INT) AS quantity,

    -- cois.unit_price = CASE WHEN coib.quantity > 0 THEN coib.sale_amount/coib.quantity ELSE NULL END
    CASE
      WHEN CAST(coib.quantity AS INT) > 0 THEN CAST(coib.sale_amount AS DECIMAL(18,2)) / CAST(coib.quantity AS DECIMAL(18,2))
      ELSE NULL
    END AS unit_price,

    -- cois.line_amount = coib.sale_amount
    CAST(coib.sale_amount AS DECIMAL(18,2)) AS line_amount,

    -- cois.currency_code = 'USD'
    'USD' AS currency_code,

    ROW_NUMBER() OVER (
      PARTITION BY coib.transaction_id, coib.product_id, coib.transaction_time
      ORDER BY CAST(coib.sale_amount AS DECIMAL(18,2)) DESC
    ) AS rn
  FROM filtered coib
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount,
  currency_code
FROM ranked
WHERE rn = 1
"""

customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

# ====================================================================================
# TARGET TABLE: etl_order_data_quality_daily_silver
# ====================================================================================
etl_order_data_quality_daily_silver_sql = """
SELECT
  -- eodqds.business_date = CAST(eodqdb.transaction_time AS DATE)
  CAST(eodqdb.transaction_time AS DATE) AS business_date,

  -- eodqds.records_received = COUNT(*)
  COUNT(1) AS records_received,

  -- eodqds.records_valid = COUNT(*) WHERE ... (implemented via SUM(CASE WHEN ... THEN 1 ELSE 0 END))
  SUM(
    CASE
      WHEN eodqdb.transaction_id IS NOT NULL
        AND eodqdb.product_id IS NOT NULL
        AND eodqdb.quantity IS NOT NULL
        AND CAST(eodqdb.quantity AS INT) > 0
        AND eodqdb.sale_amount IS NOT NULL
        AND eodqdb.transaction_time IS NOT NULL
      THEN 1 ELSE 0
    END
  ) AS records_valid,

  -- eodqds.records_rejected = records_received - records_valid
  (COUNT(1) - SUM(
    CASE
      WHEN eodqdb.transaction_id IS NOT NULL
        AND eodqdb.product_id IS NOT NULL
        AND eodqdb.quantity IS NOT NULL
        AND CAST(eodqdb.quantity AS INT) > 0
        AND eodqdb.sale_amount IS NOT NULL
        AND eodqdb.transaction_time IS NOT NULL
      THEN 1 ELSE 0
    END
  )) AS records_rejected,

  -- eodqds.validation_accuracy_pct = CASE WHEN COUNT(*) > 0 THEN (records_valid/records_received)*100 ELSE 0 END
  CASE
    WHEN COUNT(1) > 0 THEN
      (CAST(SUM(
        CASE
          WHEN eodqdb.transaction_id IS NOT NULL
            AND eodqdb.product_id IS NOT NULL
            AND eodqdb.quantity IS NOT NULL
            AND CAST(eodqdb.quantity AS INT) > 0
            AND eodqdb.sale_amount IS NOT NULL
            AND eodqdb.transaction_time IS NOT NULL
          THEN 1 ELSE 0
        END
      ) AS DECIMAL(18,2)) / CAST(COUNT(1) AS DECIMAL(18,2))) * 100
    ELSE CAST(0 AS DECIMAL(18,2))
  END AS validation_accuracy_pct,

  -- eodqds.pipeline_start_ts = MIN(eodqdb.transaction_time)
  MIN(CAST(eodqdb.transaction_time AS TIMESTAMP)) AS pipeline_start_ts,

  -- eodqds.pipeline_end_ts = MAX(eodqdb.transaction_time)
  MAX(CAST(eodqdb.transaction_time AS TIMESTAMP)) AS pipeline_end_ts,

  -- eodqds.ingestion_latency_minutes = DATEDIFF(minute, MAX(eodqdb.transaction_time), CURRENT_TIMESTAMP)
  -- Spark SQL equivalent in minutes:
  CAST(
    (unix_timestamp(CURRENT_TIMESTAMP) - unix_timestamp(MAX(CAST(eodqdb.transaction_time AS TIMESTAMP)))) / 60
    AS INT
  ) AS ingestion_latency_minutes

FROM etl_order_data_quality_daily_bronze eodqdb
GROUP BY CAST(eodqdb.transaction_time AS DATE)
"""

etl_order_data_quality_daily_silver_df = spark.sql(etl_order_data_quality_daily_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    etl_order_data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_order_data_quality_daily_silver.csv")
)

job.commit()
```