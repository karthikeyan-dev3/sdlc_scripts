```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT path format: {SOURCE_PATH}/table.{FILE_FORMAT}/)
# -----------------------------------------------------------------------------------
customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

customer_order_items_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

order_data_quality_daily_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_data_quality_daily.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
customer_orders_df.createOrReplaceTempView("customer_orders")
customer_order_items_df.createOrReplaceTempView("customer_order_items")
order_data_quality_daily_df.createOrReplaceTempView("order_data_quality_daily")

# ===================================================================================
# TARGET TABLE: gold_customer_orders
# Mapping: silver.customer_orders sco LEFT JOIN silver.customer_order_items scoi ON sco.order_id = scoi.order_id
# Transformations (EXACT from UDT):
#   - order_id = sco.order_id
#   - order_date = sco.order_date
#   - order_timestamp = sco.order_timestamp
#   - customer_id = sco.customer_id
#   - subtotal_amount = sco.sale_amount
#   - total_amount = sco.sale_amount
#   - item_count = SUM(scoi.quantity) BY sco.order_id
#   - Dedup: ROW_NUMBER on order_id (keep latest order_timestamp)
# ===================================================================================
gold_customer_orders_sql = """
WITH base AS (
  SELECT
    CAST(sco.order_id AS STRING)                       AS order_id,
    CAST(sco.order_date AS DATE)                       AS order_date,
    sco.order_timestamp                                AS order_timestamp,
    CAST(sco.customer_id AS STRING)                    AS customer_id,
    CAST(sco.sale_amount AS DECIMAL(18,2))             AS subtotal_amount,
    CAST(sco.sale_amount AS DECIMAL(18,2))             AS total_amount,
    CAST(COALESCE(SUM(CAST(scoi.quantity AS INT)), 0) AS INT) AS item_count
  FROM customer_orders sco
  LEFT JOIN customer_order_items scoi
    ON sco.order_id = scoi.order_id
  GROUP BY
    sco.order_id,
    sco.order_date,
    sco.order_timestamp,
    sco.customer_id,
    sco.sale_amount
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) AS rn
  FROM base
)
SELECT
  order_id,
  order_date,
  order_timestamp,
  customer_id,
  subtotal_amount,
  total_amount,
  item_count
FROM dedup
WHERE rn = 1
"""

gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold_customer_order_items
# Mapping: silver.customer_order_items scoi INNER JOIN silver.customer_orders sco ON scoi.order_id = sco.order_id
# Transformations (EXACT from UDT):
#   - order_id = scoi.order_id
#   - product_id = scoi.product_id
#   - quantity = scoi.quantity
#   - Dedup: ROW_NUMBER on (order_id, product_id) (keep highest quantity, then stable tie-break)
# ===================================================================================
gold_customer_order_items_sql = """
WITH base AS (
  SELECT
    CAST(scoi.order_id AS STRING)     AS order_id,
    CAST(scoi.product_id AS STRING)   AS product_id,
    CAST(scoi.quantity AS INT)        AS quantity
  FROM customer_order_items scoi
  INNER JOIN customer_orders sco
    ON scoi.order_id = sco.order_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id
      ORDER BY quantity DESC, order_id ASC, product_id ASC
    ) AS rn
  FROM base
)
SELECT
  order_id,
  product_id,
  quantity
FROM dedup
WHERE rn = 1
"""

gold_customer_order_items_df = spark.sql(gold_customer_order_items_sql)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: gold_order_data_quality_daily
# Mapping: silver.order_data_quality_daily sodqd
# Notes:
#   - UDT description mentions KPI calculations (completeness_pct, quality_score_pct),
#     but no explicit column-level UDT transformations were provided for these fields.
#   - This script loads the table as-is with light type casting and dedup by (run_date, source_system).
# ===================================================================================
gold_order_data_quality_daily_sql = """
WITH base AS (
  SELECT
    CAST(run_date AS DATE) AS run_date,
    CAST(source_system AS STRING) AS source_system,
    CAST(records_ingested AS BIGINT) AS records_ingested,
    CAST(records_loaded AS BIGINT) AS records_loaded,
    CAST(invalid_record_count AS BIGINT) AS invalid_record_count,
    CAST(missing_required_field_count AS BIGINT) AS missing_required_field_count,
    completeness_pct,
    quality_score_pct
  FROM order_data_quality_daily
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY run_date, source_system
      ORDER BY run_date DESC
    ) AS rn
  FROM base
)
SELECT
  run_date,
  source_system,
  records_ingested,
  records_loaded,
  invalid_record_count,
  missing_required_field_count,
  completeness_pct,
  quality_score_pct
FROM dedup
WHERE rn = 1
"""

gold_order_data_quality_daily_df = spark.sql(gold_order_data_quality_daily_sql)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_order_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_data_quality_daily.csv")
)

job.commit()
```