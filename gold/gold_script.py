```python
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------------------------------------------------------------
# AWS Glue Context
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init("gold_layer_build", {})

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
#    (STRICT: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/"))
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

# ===================================================================================
# TARGET TABLE: gold.gold_customer_orders
# Source: silver.customer_orders (alias sco)
# Grain: 1 row per order_id (assumed already de-duplicated in silver)
# ===================================================================================
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(sco.order_id AS STRING)                             AS order_id,
  CAST(sco.order_date AS DATE)                             AS order_date,
  CAST(sco.customer_id AS STRING)                          AS customer_id,
  CAST(sco.order_status AS STRING)                         AS order_status,
  CAST(sco.order_total_amount AS DECIMAL(18,2))            AS order_total_amount,
  CAST(sco.currency_code AS STRING)                        AS currency_code
FROM customer_orders_silver sco
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_customer_order_items
# Source: silver.customer_order_items (alias scoi)
# Grain: 1 row per order_item_id (assumed already enforced in silver)
# ===================================================================================
gold_customer_order_items_df = spark.sql("""
SELECT
  CAST(scoi.order_id AS STRING)                            AS order_id,
  CAST(scoi.order_item_id AS STRING)                       AS order_item_id,
  CAST(scoi.product_id AS STRING)                          AS product_id,
  CAST(scoi.quantity AS INT)                               AS quantity,
  CAST(scoi.unit_price AS DECIMAL(18,4))                   AS unit_price,
  CAST(scoi.line_total_amount AS DECIMAL(18,2))            AS line_total_amount
FROM customer_order_items_silver scoi
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_order_items_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_daily_order_summary
# Source: silver.customer_orders sco
# Join: LEFT JOIN silver.customer_order_items scoi ON sco.order_id = scoi.order_id
# Grain: 1 row per order_date
# Metrics:
#   total_orders          = count(distinct sco.order_id) grouped by sco.order_date
#   total_items           = coalesce(sum(scoi.quantity),0) grouped by sco.order_date
#   total_revenue_amount  = sum(sco.order_total_amount) grouped by sco.order_date
# ===================================================================================
gold_daily_order_summary_df = spark.sql("""
SELECT
  CAST(sco.order_date AS DATE)                                            AS order_date,
  CAST(COUNT(DISTINCT sco.order_id) AS BIGINT)                             AS total_orders,
  CAST(COALESCE(SUM(CAST(scoi.quantity AS BIGINT)), 0) AS BIGINT)          AS total_items,
  CAST(SUM(CAST(sco.order_total_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue_amount
FROM customer_orders_silver sco
LEFT JOIN customer_order_items_silver scoi
  ON sco.order_id = scoi.order_id
GROUP BY CAST(sco.order_date AS DATE)
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_order_summary_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_order_summary.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_data_quality_daily
# Source: silver.data_quality_daily (alias sdq)
# Grain: 1 row per load_date
# ===================================================================================
gold_data_quality_daily_df = spark.sql("""
SELECT
  CAST(sdq.load_date AS DATE)                             AS load_date,
  CAST(sdq.source_record_count AS BIGINT)                 AS source_record_count,
  CAST(sdq.loaded_order_count AS BIGINT)                  AS loaded_order_count,
  CAST(sdq.loaded_order_item_count AS BIGINT)             AS loaded_order_item_count,
  CAST(sdq.rejected_record_count AS BIGINT)               AS rejected_record_count,
  CAST(sdq.data_accuracy_pct AS DECIMAL(9,4))             AS data_accuracy_pct
FROM data_quality_daily_silver sdq
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_data_quality_daily_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_daily.csv")
)

job.commit()
```