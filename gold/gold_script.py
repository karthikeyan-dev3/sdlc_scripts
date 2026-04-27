```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue job bootstrap
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

# -----------------------------------------------------------------------------------
# Source reads (S3) + Temp Views
# NOTE: Paths must follow: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)
orders_silver_df.createOrReplaceTempView("orders_silver")

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

orders_data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_data_quality_daily_silver.{FILE_FORMAT}/")
)
orders_data_quality_daily_silver_df.createOrReplaceTempView("orders_data_quality_daily_silver")

# -----------------------------------------------------------------------------------
# Target: gold.gold_orders
# Description:
# - Select from silver.orders_silver at order header grain
# - Map order_id, order_date, order_status, order_total_amount, currency_code,
#   source_system, ingestion_date directly from os
# - Set customer_id = NULL
# -----------------------------------------------------------------------------------
gold_orders_df = spark.sql("""
SELECT
  CAST(os.order_id AS STRING)                        AS order_id,
  CAST(os.order_date AS DATE)                        AS order_date,
  CAST(os.order_status AS STRING)                    AS order_status,
  CAST(os.order_total_amount AS DECIMAL(38, 10))     AS order_total_amount,
  CAST(os.currency_code AS STRING)                   AS currency_code,
  CAST(os.source_system AS STRING)                   AS source_system,
  CAST(os.ingestion_date AS DATE)                    AS ingestion_date,
  CAST(NULL AS STRING)                               AS customer_id
FROM orders_silver os
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_order_items
# Description:
# - Select from silver.order_items_silver at order line grain
# - Map order_id, order_item_id, product_id, quantity, unit_price, line_amount,
#   currency_code, ingestion_date directly from ois
# -----------------------------------------------------------------------------------
gold_order_items_df = spark.sql("""
SELECT
  CAST(ois.order_id AS STRING)                    AS order_id,
  CAST(ois.order_item_id AS INT)                  AS order_item_id,
  CAST(ois.product_id AS STRING)                  AS product_id,
  CAST(ois.quantity AS INT)                       AS quantity,
  CAST(ois.unit_price AS DECIMAL(38, 10))         AS unit_price,
  CAST(ois.line_amount AS DECIMAL(38, 10))        AS line_amount,
  CAST(ois.currency_code AS STRING)               AS currency_code,
  CAST(ois.ingestion_date AS DATE)                AS ingestion_date
FROM order_items_silver ois
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold.gold_orders_data_quality_daily
# Description:
# - Select from silver.orders_data_quality_daily_silver at (run_date, source_system) grain
# - Map run_date, source_system, total_records, valid_records, invalid_records,
#   accuracy_percent, load_start_ts, load_end_ts, latency_minutes directly from odqds
# -----------------------------------------------------------------------------------
gold_orders_data_quality_daily_df = spark.sql("""
SELECT
  CAST(odqds.run_date AS DATE)                     AS run_date,
  CAST(odqds.source_system AS STRING)              AS source_system,
  CAST(odqds.total_records AS INT)                 AS total_records,
  CAST(odqds.valid_records AS INT)                 AS valid_records,
  CAST(odqds.invalid_records AS INT)               AS invalid_records,
  CAST(odqds.accuracy_percent AS DECIMAL(38, 10))  AS accuracy_percent,
  CAST(odqds.load_start_ts AS TIMESTAMP)           AS load_start_ts,
  CAST(odqds.load_end_ts AS TIMESTAMP)             AS load_end_ts,
  CAST(odqds.latency_minutes AS INT)               AS latency_minutes
FROM orders_data_quality_daily_silver odqds
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_orders_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders_data_quality_daily.csv")
)

job.commit()
```