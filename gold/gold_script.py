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
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended for stable CSV reads (adjust if your silver files differ)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# SOURCE READS + TEMP VIEWS
# -----------------------------------------------------------------------------------

# silver.orders_silver
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)
orders_silver_df.createOrReplaceTempView("orders_silver")

# silver.order_items_silver
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# silver.etl_data_quality_daily_silver
etl_data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_daily_silver.{FILE_FORMAT}/")
)
etl_data_quality_daily_silver_df.createOrReplaceTempView("etl_data_quality_daily_silver")

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_orders
# One row per order_id (dedup with ROW_NUMBER)
# Transformations: CAST, COALESCE, TRIM, UPPER, LOWER, DATE (as applicable)
# -----------------------------------------------------------------------------------
gold_orders_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(os.order_id) AS STRING)                           AS order_id,
    CAST(DATE(os.order_date) AS DATE)                           AS order_date,
    CAST(TRIM(os.order_status) AS STRING)                       AS order_status,
    CAST(TRIM(os.customer_id) AS STRING)                        AS customer_id,
    CAST(UPPER(TRIM(os.currency_code)) AS STRING)               AS currency_code,
    CAST(os.order_total_amount AS DECIMAL(38, 10))              AS order_total_amount,
    CAST(DATE(os.ingestion_date) AS DATE)                       AS ingestion_date
  FROM orders_silver os
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY ingestion_date DESC, order_date DESC
    ) AS rn
  FROM base
)
SELECT
  order_id,
  order_date,
  order_status,
  customer_id,
  currency_code,
  order_total_amount,
  ingestion_date
FROM dedup
WHERE rn = 1
"""

gold_orders_df = spark.sql(gold_orders_sql)

(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_order_items
# Transformations: CAST, COALESCE, TRIM, UPPER, LOWER, DATE (as applicable)
# -----------------------------------------------------------------------------------
gold_order_items_sql = """
SELECT
  CAST(TRIM(ois.order_id) AS STRING)                 AS order_id,
  CAST(TRIM(ois.order_item_id) AS STRING)            AS order_item_id,
  CAST(TRIM(ois.product_id) AS STRING)               AS product_id,
  CAST(ois.quantity AS INT)                          AS quantity,
  CAST(ois.unit_price AS DECIMAL(38, 10))            AS unit_price,
  CAST(ois.line_total_amount AS DECIMAL(38, 10))     AS line_total_amount
FROM order_items_silver ois
"""

gold_order_items_df = spark.sql(gold_order_items_sql)

(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_etl_data_quality_daily
# Dedup with ROW_NUMBER by (ingestion_date, source_system)
# Transformations: CAST, COALESCE, TRIM, UPPER, LOWER, DATE (as applicable)
# -----------------------------------------------------------------------------------
gold_etl_data_quality_daily_sql = """
WITH base AS (
  SELECT
    CAST(DATE(dqds.ingestion_date) AS DATE)                      AS ingestion_date,
    CAST(LOWER(TRIM(dqds.source_system)) AS STRING)              AS source_system,
    CAST(dqds.total_records_ingested AS BIGINT)                  AS total_records_ingested,
    CAST(dqds.total_records_valid AS BIGINT)                     AS total_records_valid,
    CAST(dqds.total_records_rejected AS BIGINT)                  AS total_records_rejected,
    CAST(dqds.accuracy_rate_pct AS DECIMAL(38, 10))              AS accuracy_rate_pct,
    CAST(dqds.pipeline_start_ts AS TIMESTAMP)                    AS pipeline_start_ts,
    CAST(dqds.pipeline_end_ts AS TIMESTAMP)                      AS pipeline_end_ts,
    CAST(dqds.processing_time_minutes AS INT)                    AS processing_time_minutes
  FROM etl_data_quality_daily_silver dqds
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ingestion_date, source_system
      ORDER BY pipeline_end_ts DESC, pipeline_start_ts DESC
    ) AS rn
  FROM base
)
SELECT
  ingestion_date,
  source_system,
  total_records_ingested,
  total_records_valid,
  total_records_rejected,
  accuracy_rate_pct,
  pipeline_start_ts,
  pipeline_end_ts,
  processing_time_minutes
FROM dedup
WHERE rn = 1
"""

gold_etl_data_quality_daily_df = spark.sql(gold_etl_data_quality_daily_sql)

(
    gold_etl_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_data_quality_daily.csv")
)

job.commit()
```