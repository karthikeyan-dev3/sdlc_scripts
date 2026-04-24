```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Job Setup
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

# ====================================================================================
# Source Read: silver.customer_orders (sco)
# ====================================================================================
df_sco = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_sco.createOrReplaceTempView("customer_orders")

# ====================================================================================
# Target: gold.gold_customer_orders (gco)
#   Transformations (EXACT from UDT):
#     gco.order_id = sco.order_id
#     gco.order_date = sco.order_date
#     gco.customer_id = sco.customer_id
#     gco.order_total_amount = sco.order_total_amount
#     gco.created_at = sco.created_at
#     gco.updated_at = sco.updated_at
#     gco.load_date = sco.load_date
# ====================================================================================
df_gco = spark.sql("""
SELECT
  CAST(sco.order_id AS STRING)            AS order_id,
  CAST(sco.order_date AS DATE)            AS order_date,
  CAST(sco.customer_id AS STRING)         AS customer_id,
  CAST(sco.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
  CAST(sco.created_at AS TIMESTAMP)       AS created_at,
  CAST(sco.updated_at AS TIMESTAMP)       AS updated_at,
  CAST(sco.load_date AS DATE)             AS load_date
FROM customer_orders sco
""")

(
    df_gco.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ====================================================================================
# Source Read: silver.customer_order_items (scoi)
# ====================================================================================
df_scoi = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_scoi.createOrReplaceTempView("customer_order_items")

# ====================================================================================
# Target: gold.gold_customer_order_items (gcoi)
#   Transformations (EXACT from UDT):
#     gcoi.order_id = scoi.order_id
#     gcoi.order_item_id = scoi.order_item_id
#     gcoi.product_id = scoi.product_id
#     gcoi.quantity = scoi.quantity
#     gcoi.unit_price = scoi.unit_price
#     gcoi.line_amount = scoi.line_amount
#     gcoi.load_date = scoi.load_date
# ====================================================================================
df_gcoi = spark.sql("""
SELECT
  CAST(scoi.order_id AS STRING)                 AS order_id,
  CAST(scoi.order_item_id AS STRING)            AS order_item_id,
  CAST(scoi.product_id AS STRING)               AS product_id,
  CAST(scoi.quantity AS INT)                    AS quantity,
  CAST(scoi.unit_price AS DECIMAL(38, 10))      AS unit_price,
  CAST(scoi.line_amount AS DECIMAL(38, 10))     AS line_amount,
  CAST(scoi.load_date AS DATE)                  AS load_date
FROM customer_order_items scoi
""")

(
    df_gcoi.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ====================================================================================
# Source Read: silver.data_quality_daily_orders (sdqdo)
# ====================================================================================
df_sdqdo = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_orders.{FILE_FORMAT}/")
)
df_sdqdo.createOrReplaceTempView("data_quality_daily_orders")

# ====================================================================================
# Target: gold.gold_data_quality_daily_orders (gdqdo)
#   Transformations (EXACT from UDT):
#     gdqdo.load_date = sdqdo.load_date
#     gdqdo.total_records = sdqdo.total_records
#     gdqdo.valid_records = sdqdo.valid_records
#     gdqdo.invalid_records = sdqdo.invalid_records
#     gdqdo.completeness_score_pct = sdqdo.completeness_score_pct
#     gdqdo.accuracy_score_pct = sdqdo.accuracy_score_pct
#     gdqdo.validation_run_timestamp = sdqdo.validation_run_timestamp
# ====================================================================================
df_gdqdo = spark.sql("""
SELECT
  CAST(sdqdo.load_date AS DATE)                      AS load_date,
  CAST(sdqdo.total_records AS INT)                   AS total_records,
  CAST(sdqdo.valid_records AS INT)                   AS valid_records,
  CAST(sdqdo.invalid_records AS INT)                 AS invalid_records,
  CAST(sdqdo.completeness_score_pct AS DECIMAL(38, 10)) AS completeness_score_pct,
  CAST(sdqdo.accuracy_score_pct AS DECIMAL(38, 10))     AS accuracy_score_pct,
  CAST(sdqdo.validation_run_timestamp AS TIMESTAMP)  AS validation_run_timestamp
FROM data_quality_daily_orders sdqdo
""")

(
    df_gdqdo.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_daily_orders.csv")
)

job.commit()
```