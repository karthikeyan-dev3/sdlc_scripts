```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue init
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ====================================================================================
# SOURCE: silver.orders_silver  -> TARGET: gold.gold_orders
# ====================================================================================

# 1) Read source table from S3 (STRICT PATH FORMAT)
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
orders_silver_df.createOrReplaceTempView("orders_silver")

# 3) Transform using Spark SQL (casts + optional ROW_NUMBER dedup pattern)
gold_orders_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(os.order_id AS STRING)                          AS order_id,
        CAST(os.order_date AS DATE)                          AS order_date,
        CAST(os.customer_id AS STRING)                       AS customer_id,
        CAST(os.order_status AS STRING)                      AS order_status,
        CAST(os.order_total_amount AS DECIMAL(18,2))         AS order_total_amount,
        CAST(os.currency_code AS STRING)                     AS currency_code,
        CAST(os.payment_method AS STRING)                    AS payment_method,
        CAST(os.shipping_method AS STRING)                   AS shipping_method,
        CAST(os.ship_to_country_code AS STRING)              AS ship_to_country_code,
        CAST(os.record_source AS STRING)                     AS record_source,
        CAST(os.ingestion_date AS DATE)                      AS ingestion_date,
        ROW_NUMBER() OVER (
          PARTITION BY os.order_id
          ORDER BY os.ingestion_date DESC
        ) AS rn
      FROM orders_silver os
    )
    SELECT
      order_id,
      order_date,
      customer_id,
      order_status,
      order_total_amount,
      currency_code,
      payment_method,
      shipping_method,
      ship_to_country_code,
      record_source,
      ingestion_date
    FROM base
    WHERE rn = 1
    """
)

# 4) Write output as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders.csv")
)

# ====================================================================================
# SOURCE: silver.order_items_silver  -> TARGET: gold.gold_order_items
# ====================================================================================

# 1) Read source table from S3 (STRICT PATH FORMAT)
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# 3) Transform using Spark SQL (casts + optional ROW_NUMBER dedup pattern)
gold_order_items_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(ois.order_id AS STRING)                    AS order_id,
        CAST(ois.order_item_id AS STRING)               AS order_item_id,
        CAST(ois.product_id AS STRING)                  AS product_id,
        CAST(ois.quantity AS INT)                       AS quantity,
        CAST(ois.unit_price_amount AS DECIMAL(18,2))    AS unit_price_amount,
        CAST(ois.line_total_amount AS DECIMAL(18,2))    AS line_total_amount,
        CAST(ois.ingestion_date AS DATE)                AS ingestion_date,
        ROW_NUMBER() OVER (
          PARTITION BY ois.order_id, ois.order_item_id
          ORDER BY ois.ingestion_date DESC
        ) AS rn
      FROM order_items_silver ois
    )
    SELECT
      order_id,
      order_item_id,
      product_id,
      quantity,
      unit_price_amount,
      line_total_amount,
      ingestion_date
    FROM base
    WHERE rn = 1
    """
)

# 4) Write output as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# ====================================================================================
# SOURCE: silver.daily_etl_run_metrics_silver  -> TARGET: gold.gold_daily_etl_run_metrics
# ====================================================================================

# 1) Read source table from S3 (STRICT PATH FORMAT)
daily_etl_run_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/daily_etl_run_metrics_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
daily_etl_run_metrics_silver_df.createOrReplaceTempView("daily_etl_run_metrics_silver")

# 3) Transform using Spark SQL (casts + optional ROW_NUMBER dedup pattern)
gold_daily_etl_run_metrics_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(derms.run_date AS DATE)             AS run_date,
        CAST(derms.dataset_name AS STRING)       AS dataset_name,
        CAST(derms.records_read_count AS BIGINT) AS records_read_count,
        CAST(derms.records_loaded_count AS BIGINT) AS records_loaded_count,
        CAST(derms.records_rejected_count AS BIGINT) AS records_rejected_count,
        CAST(derms.load_start_ts AS TIMESTAMP)   AS load_start_ts,
        CAST(derms.load_end_ts AS TIMESTAMP)     AS load_end_ts,
        CAST(derms.duration_minutes AS INT)      AS duration_minutes,
        ROW_NUMBER() OVER (
          PARTITION BY derms.run_date, derms.dataset_name
          ORDER BY derms.load_end_ts DESC
        ) AS rn
      FROM daily_etl_run_metrics_silver derms
    )
    SELECT
      run_date,
      dataset_name,
      records_read_count,
      records_loaded_count,
      records_rejected_count,
      load_start_ts,
      load_end_ts,
      duration_minutes
    FROM base
    WHERE rn = 1
    """
)

# 4) Write output as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_etl_run_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_etl_run_metrics.csv")
)

job.commit()
```