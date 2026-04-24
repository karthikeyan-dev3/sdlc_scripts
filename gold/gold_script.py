```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------------------
# Glue job setup
# ---------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------------------
# Parameters (as provided)
# ---------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Ensure single file output per table
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Common CSV options
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": '"'
}

# =======================================================================================
# Source reads + temp views
# =======================================================================================

# silver.customer_orders_silver cos
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# silver.customer_order_items_silver cois
df_customer_order_items_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
df_customer_order_items_silver.createOrReplaceTempView("customer_order_items_silver")

# silver.etl_order_data_quality_daily_silver eodqds
df_etl_order_data_quality_daily_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/etl_order_data_quality_daily_silver.{FILE_FORMAT}/")
)
df_etl_order_data_quality_daily_silver.createOrReplaceTempView("etl_order_data_quality_daily_silver")

# =======================================================================================
# Target: gold.gold_customer_orders
# =======================================================================================
sql_gold_customer_orders = """
SELECT
  CAST(cos.order_id AS STRING)                       AS order_id,
  CAST(cos.order_date AS DATE)                       AS order_date,
  CAST(cos.customer_id AS STRING)                    AS customer_id,
  CAST(cos.order_status AS STRING)                   AS order_status,
  CAST(cos.order_total_amount AS DECIMAL(38, 10))    AS order_total_amount,
  CAST(cos.currency_code AS STRING)                  AS currency_code,
  CAST(cos.source_system AS STRING)                  AS source_system,
  CAST(cos.ingestion_date AS DATE)                   AS ingestion_date,
  CAST(cos.loaded_at AS TIMESTAMP)                   AS loaded_at
FROM customer_orders_silver cos
"""

df_gold_customer_orders = spark.sql(sql_gold_customer_orders)

(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# =======================================================================================
# Target: gold.gold_customer_order_items
# =======================================================================================
sql_gold_customer_order_items = """
SELECT
  CAST(cois.order_id AS STRING)                      AS order_id,
  CAST(cois.order_item_id AS STRING)                 AS order_item_id,
  CAST(cois.product_id AS STRING)                    AS product_id,
  CAST(cois.quantity AS INT)                         AS quantity,
  CAST(cois.unit_price AS DECIMAL(38, 10))           AS unit_price,
  CAST(cois.line_amount AS DECIMAL(38, 10))          AS line_amount,
  CAST(cois.currency_code AS STRING)                 AS currency_code
FROM customer_order_items_silver cois
"""

df_gold_customer_order_items = spark.sql(sql_gold_customer_order_items)

(
    df_gold_customer_order_items.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# =======================================================================================
# Target: gold.gold_etl_order_data_quality_daily
# =======================================================================================
sql_gold_etl_order_data_quality_daily = """
SELECT
  CAST(eodqds.business_date AS DATE)                     AS business_date,
  CAST(eodqds.records_received AS INT)                   AS records_received,
  CAST(eodqds.records_valid AS INT)                      AS records_valid,
  CAST(eodqds.records_rejected AS INT)                   AS records_rejected,
  CAST(eodqds.validation_accuracy_pct AS DECIMAL(38, 10)) AS validation_accuracy_pct,
  CAST(eodqds.pipeline_start_ts AS TIMESTAMP)            AS pipeline_start_ts,
  CAST(eodqds.pipeline_end_ts AS TIMESTAMP)              AS pipeline_end_ts,
  CAST(eodqds.ingestion_latency_minutes AS INT)          AS ingestion_latency_minutes
FROM etl_order_data_quality_daily_silver eodqds
"""

df_gold_etl_order_data_quality_daily = spark.sql(sql_gold_etl_order_data_quality_daily)

(
    df_gold_etl_order_data_quality_daily.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_order_data_quality_daily.csv")
)

job.commit()
```