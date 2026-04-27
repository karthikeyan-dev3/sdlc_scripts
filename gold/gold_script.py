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
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ===================================================================================
# TABLE: gold_orders  (source: silver.orders_silver os)
# ===================================================================================

# 1) Read source table
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
orders_silver_df.createOrReplaceTempView("orders_silver")

# 3) SQL transformation (apply exact UDT mappings + simple standardization + dedup)
gold_orders_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(os.order_id AS STRING)                         AS order_id,
        CAST(os.order_ts AS TIMESTAMP)                      AS order_ts,
        CAST(os.order_date AS DATE)                         AS order_date,
        CAST(os.customer_id AS STRING)                      AS customer_id,
        CAST(os.order_amount AS DECIMAL(38,10))             AS order_amount,
        CAST(TRIM(UPPER(os.currency_code)) AS STRING)       AS currency_code,
        CAST(TRIM(UPPER(os.order_status)) AS STRING)        AS order_status,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(os.order_id AS STRING)
          ORDER BY CAST(os.order_ts AS TIMESTAMP) DESC
        ) AS rn
      FROM orders_silver os
    )
    SELECT
      order_id,
      order_ts,
      order_date,
      customer_id,
      order_amount,
      currency_code,
      order_status
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders.csv")
)

# ===================================================================================
# TABLE: gold_order_items  (source: silver.order_items_silver ois)
# ===================================================================================

# 1) Read source table
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# 3) SQL transformation (apply exact UDT mappings + dedup)
gold_order_items_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(ois.order_id AS STRING)                    AS order_id,
        CAST(ois.order_item_id AS STRING)               AS order_item_id,
        CAST(ois.product_id AS STRING)                  AS product_id,
        CAST(ois.quantity AS INT)                       AS quantity,
        CAST(ois.unit_price AS DECIMAL(38,10))          AS unit_price,
        CAST(ois.line_amount AS DECIMAL(38,10))         AS line_amount,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(ois.order_item_id AS STRING)
          ORDER BY CAST(ois.order_id AS STRING) ASC
        ) AS rn
      FROM order_items_silver ois
    )
    SELECT
      order_id,
      order_item_id,
      product_id,
      quantity,
      unit_price,
      line_amount
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# ===================================================================================
# TABLE: gold_customer_orders_daily  (source: silver.customer_orders_daily_silver cods)
# ===================================================================================

# 1) Read source table
customer_orders_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_daily_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_orders_daily_silver_df.createOrReplaceTempView("customer_orders_daily_silver")

# 3) SQL transformation (apply exact UDT mappings + dedup)
gold_customer_orders_daily_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(cods.order_date AS DATE)                 AS order_date,
        CAST(cods.customer_id AS STRING)              AS customer_id,
        CAST(cods.total_orders AS INT)                AS total_orders,
        CAST(cods.total_items AS INT)                 AS total_items,
        CAST(cods.total_order_amount AS DECIMAL(38,10)) AS total_order_amount,
        CAST(cods.avg_order_amount AS DECIMAL(38,10)) AS avg_order_amount,
        CAST(cods.first_order_ts AS TIMESTAMP)        AS first_order_ts,
        CAST(cods.last_order_ts AS TIMESTAMP)         AS last_order_ts,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(cods.order_date AS DATE), CAST(cods.customer_id AS STRING)
          ORDER BY CAST(cods.last_order_ts AS TIMESTAMP) DESC
        ) AS rn
      FROM customer_orders_daily_silver cods
    )
    SELECT
      order_date,
      customer_id,
      total_orders,
      total_items,
      total_order_amount,
      avg_order_amount,
      first_order_ts,
      last_order_ts
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    gold_customer_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders_daily.csv")
)

# ===================================================================================
# TABLE: gold_etl_data_quality_daily  (source: silver.etl_data_quality_daily_silver dqds)
# ===================================================================================

# 1) Read source table
etl_data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_daily_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
etl_data_quality_daily_silver_df.createOrReplaceTempView("etl_data_quality_daily_silver")

# 3) SQL transformation (apply exact UDT mappings + dedup)
gold_etl_data_quality_daily_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(dqds.run_date AS DATE)                      AS run_date,
        CAST(TRIM(LOWER(dqds.source_system)) AS STRING)  AS source_system,
        CAST(TRIM(LOWER(dqds.dataset_name)) AS STRING)   AS dataset_name,
        CAST(dqds.records_ingested AS INT)               AS records_ingested,
        CAST(dqds.records_valid AS INT)                  AS records_valid,
        CAST(dqds.records_rejected AS INT)               AS records_rejected,
        CAST(dqds.data_quality_score_pct AS DECIMAL(38,10)) AS data_quality_score_pct,
        CAST(dqds.sla_on_time_flag AS INT)               AS sla_on_time_flag,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(dqds.run_date AS DATE),
                       CAST(TRIM(LOWER(dqds.source_system)) AS STRING),
                       CAST(TRIM(LOWER(dqds.dataset_name)) AS STRING)
          ORDER BY CAST(dqds.data_quality_score_pct AS DECIMAL(38,10)) DESC
        ) AS rn
      FROM etl_data_quality_daily_silver dqds
    )
    SELECT
      run_date,
      source_system,
      dataset_name,
      records_ingested,
      records_valid,
      records_rejected,
      data_quality_score_pct,
      sla_on_time_flag
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    gold_etl_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_data_quality_daily.csv")
)

job.commit()
```