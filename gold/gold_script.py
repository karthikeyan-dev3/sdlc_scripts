```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Boilerplate
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV options (safe defaults)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE"
}

csv_write_options = {
    "header": "true",
    "quote": '"',
    "escape": '"'
}

# ====================================================================================
# SOURCE: silver.customer_orders
# TARGET: gold.gold_customer_orders
# ====================================================================================
customer_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_src_df.createOrReplaceTempView("customer_orders")

gold_customer_orders_df = spark.sql("""
SELECT
    CAST(sco.order_id AS STRING)                          AS order_id,
    CAST(sco.order_date AS DATE)                          AS order_date,
    CAST(sco.customer_id AS STRING)                       AS customer_id,
    CAST(sco.order_status AS STRING)                      AS order_status,
    CAST(sco.currency_code AS STRING)                     AS currency_code,
    CAST(sco.order_total_amount AS DECIMAL(38, 10))       AS order_total_amount,
    CAST(sco.source_file_date AS DATE)                    AS source_file_date
FROM customer_orders sco
""")

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ====================================================================================
# SOURCE: silver.customer_order_items
# TARGET: gold.gold_customer_order_items
# ====================================================================================
customer_order_items_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
customer_order_items_src_df.createOrReplaceTempView("customer_order_items")

gold_customer_order_items_df = spark.sql("""
SELECT
    CAST(scoi.order_id AS STRING)                         AS order_id,
    CAST(scoi.order_item_id AS STRING)                    AS order_item_id,
    CAST(scoi.product_id AS STRING)                       AS product_id,
    CAST(scoi.quantity AS INT)                            AS quantity,
    CAST(scoi.unit_price_amount AS DECIMAL(38, 10))       AS unit_price_amount,
    CAST(scoi.line_total_amount AS DECIMAL(38, 10))       AS line_total_amount
FROM customer_order_items scoi
""")

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ====================================================================================
# SOURCE: silver.daily_order_data_quality
# TARGET: gold.gold_daily_order_data_quality
# ====================================================================================
daily_order_dq_src_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/daily_order_data_quality.{FILE_FORMAT}/")
)
daily_order_dq_src_df.createOrReplaceTempView("daily_order_data_quality")

gold_daily_order_data_quality_df = spark.sql("""
SELECT
    CAST(sdodq.data_date AS DATE)                         AS data_date,
    CAST(sdodq.total_orders_count AS INT)                 AS total_orders_count,
    CAST(sdodq.valid_orders_count AS INT)                 AS valid_orders_count,
    CAST(sdodq.invalid_orders_count AS INT)               AS invalid_orders_count,
    CAST(sdodq.duplicate_orders_count AS INT)             AS duplicate_orders_count,
    CAST(sdodq.dq_pass_rate AS DECIMAL(38, 10))           AS dq_pass_rate
FROM daily_order_data_quality sdodq
""")

(
    gold_daily_order_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_daily_order_data_quality.csv")
)

job.commit()
```