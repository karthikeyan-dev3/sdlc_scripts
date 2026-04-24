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

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Common CSV options (kept simple/explicit)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "multiLine": "false",
    "escape": "\""
}

# ===================================================================================
# TARGET TABLE: gold.gold_customer_orders
# Source: silver.customer_orders_silver cos
# Dedup: one row per order_id (ROW_NUMBER)
# ===================================================================================

# 1) Read source table
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (incl. ROW_NUMBER for dedup)
gold_customer_orders_df = spark.sql("""
WITH ranked AS (
    SELECT
        cos.*,
        ROW_NUMBER() OVER (
            PARTITION BY cos.order_id
            ORDER BY cos.order_id
        ) AS rn
    FROM customer_orders_silver cos
)
SELECT
    CAST(cos.order_id AS STRING)              AS order_id,
    CAST(cos.order_date AS DATE)              AS order_date,
    CAST(cos.customer_id AS STRING)           AS customer_id,
    CAST(cos.order_status AS STRING)          AS order_status,
    CAST(cos.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
    CAST(cos.currency_code AS STRING)         AS currency_code
FROM ranked cos
WHERE cos.rn = 1
""")

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_order_items
# Source: silver.order_items_silver ois
# Dedup: one row per (order_id, product_id) (ROW_NUMBER)
# ===================================================================================

# 1) Read source table
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# 3) Transform using Spark SQL (incl. ROW_NUMBER for dedup)
gold_order_items_df = spark.sql("""
WITH ranked AS (
    SELECT
        ois.*,
        ROW_NUMBER() OVER (
            PARTITION BY ois.order_id, ois.product_id
            ORDER BY ois.order_item_id
        ) AS rn
    FROM order_items_silver ois
)
SELECT
    CAST(ois.order_id AS STRING)              AS order_id,
    CAST(ois.order_item_id AS STRING)         AS order_item_id,
    CAST(ois.product_id AS STRING)            AS product_id,
    CAST(ois.quantity AS INT)                 AS quantity,
    CAST(ois.unit_price AS DECIMAL(38, 10))   AS unit_price,
    CAST(ois.line_amount AS DECIMAL(38, 10))  AS line_amount
FROM ranked ois
WHERE ois.rn = 1
""")

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_data_quality_daily
# Source: silver.data_quality_daily_silver dqds
# Dedup: one row per (load_date, source_file_date) (ROW_NUMBER)
# ===================================================================================

# 1) Read source table
data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/data_quality_daily_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

# 3) Transform using Spark SQL (incl. ROW_NUMBER for dedup)
gold_data_quality_daily_df = spark.sql("""
WITH ranked AS (
    SELECT
        dqds.*,
        ROW_NUMBER() OVER (
            PARTITION BY dqds.load_date, dqds.source_file_date
            ORDER BY dqds.load_date, dqds.source_file_date
        ) AS rn
    FROM data_quality_daily_silver dqds
)
SELECT
    CAST(dqds.load_date AS DATE)              AS load_date,
    CAST(dqds.source_file_date AS DATE)       AS source_file_date,
    CAST(dqds.records_ingested AS BIGINT)     AS records_ingested,
    CAST(dqds.records_loaded AS BIGINT)       AS records_loaded,
    CAST(dqds.records_rejected AS BIGINT)     AS records_rejected,
    CAST(dqds.dq_pass_rate AS DECIMAL(38, 10)) AS dq_pass_rate,
    CAST(dqds.load_start_ts AS TIMESTAMP)     AS load_start_ts,
    CAST(dqds.load_end_ts AS TIMESTAMP)       AS load_end_ts
FROM ranked dqds
WHERE dqds.rn = 1
""")

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_daily.csv")
)

job.commit()
```