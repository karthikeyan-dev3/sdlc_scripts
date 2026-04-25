```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue setup
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

read_format = FILE_FORMAT.lower()

# Recommended for consistent CSV parsing (adjust if your silver layer differs)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# -----------------------------------------------------------------------------------
# SOURCE READS + TEMP VIEWS
# -----------------------------------------------------------------------------------

# silver.customer_orders_silver cos
cos_df = (
    spark.read.format(read_format)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
cos_df.createOrReplaceTempView("customer_orders_silver")

# silver.customer_order_items_silver cois
cois_df = (
    spark.read.format(read_format)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
cois_df.createOrReplaceTempView("customer_order_items_silver")

# silver.customer_silver cs
cs_df = (
    spark.read.format(read_format)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_silver.{FILE_FORMAT}/")
)
cs_df.createOrReplaceTempView("customer_silver")

# silver.product_silver ps
ps_df = (
    spark.read.format(read_format)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("product_silver")

# silver.etl_load_audit_orders_silver elaos
elaos_df = (
    spark.read.format(read_format)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/etl_load_audit_orders_silver.{FILE_FORMAT}/")
)
elaos_df.createOrReplaceTempView("etl_load_audit_orders_silver")

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_customer_orders (gco)
# Description: Map columns directly from cos
# -----------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(cos.order_id AS STRING)                AS order_id,
  CAST(cos.order_date AS DATE)                AS order_date,
  CAST(cos.customer_id AS STRING)             AS customer_id,
  CAST(cos.order_status AS STRING)            AS order_status,
  CAST(cos.order_total_amount AS DECIMAL(18,2)) AS order_total_amount,
  CAST(cos.currency_code AS STRING)           AS currency_code,
  CAST(cos.source_system AS STRING)           AS source_system,
  CAST(cos.ingested_at AS TIMESTAMP)          AS ingested_at
FROM customer_orders_silver cos
""")

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_customer_order_items (gcoi)
# Description: Map columns directly from cois
# -----------------------------------------------------------------------------------
gold_customer_order_items_df = spark.sql("""
SELECT
  CAST(cois.order_item_id AS STRING)            AS order_item_id,
  CAST(cois.order_id AS STRING)                 AS order_id,
  CAST(cois.product_id AS STRING)               AS product_id,
  CAST(cois.quantity AS INT)                    AS quantity,
  CAST(cois.unit_price AS DECIMAL(18,2))        AS unit_price,
  CAST(cois.line_amount AS DECIMAL(18,2))       AS line_amount,
  CAST(cois.currency_code AS STRING)            AS currency_code
FROM customer_order_items_silver cois
""")

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_customer (gc)
# Description: Map columns directly from cs
# -----------------------------------------------------------------------------------
gold_customer_df = spark.sql("""
SELECT
  CAST(cs.customer_id AS STRING)           AS customer_id,
  CAST(cs.customer_name AS STRING)         AS customer_name,
  CAST(cs.customer_email AS STRING)        AS customer_email,
  CAST(cs.customer_created_date AS DATE)   AS customer_created_date,
  CAST(cs.customer_status AS STRING)       AS customer_status
FROM customer_silver cs
""")

(
    gold_customer_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer.csv")
)

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_product (gp)
# Description: Map columns directly from ps
# -----------------------------------------------------------------------------------
gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)        AS product_id,
  CAST(ps.product_name AS STRING)      AS product_name,
  CAST(ps.product_category AS STRING)  AS product_category,
  CAST(ps.product_status AS STRING)    AS product_status
FROM product_silver ps
""")

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_etl_load_audit_orders (gelao)
# Description: Map columns directly from elaos
# -----------------------------------------------------------------------------------
gold_etl_load_audit_orders_df = spark.sql("""
SELECT
  CAST(elaos.load_id AS STRING)                 AS load_id,
  CAST(elaos.data_date AS DATE)                 AS data_date,
  CAST(elaos.source_bucket AS STRING)           AS source_bucket,
  CAST(elaos.source_object_prefix AS STRING)    AS source_object_prefix,
  CAST(elaos.records_received AS BIGINT)        AS records_received,
  CAST(elaos.records_loaded AS BIGINT)          AS records_loaded,
  CAST(elaos.records_rejected AS BIGINT)        AS records_rejected,
  CAST(elaos.load_start_ts AS TIMESTAMP)        AS load_start_ts,
  CAST(elaos.load_end_ts AS TIMESTAMP)          AS load_end_ts,
  CAST(elaos.load_status AS STRING)             AS load_status
FROM etl_load_audit_orders_silver elaos
""")

(
    gold_etl_load_audit_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_load_audit_orders.csv")
)

job.commit()
```