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

# Recommended for stable CSV output characteristics
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ===================================================================================
# SOURCE TABLES (read from S3) + TEMP VIEWS
# ===================================================================================

# silver.customer_orders (alias: sco)
df_sco = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_sco.createOrReplaceTempView("customer_orders")

# silver.customer_order_items (alias: scoi)
df_scoi = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_scoi.createOrReplaceTempView("customer_order_items")

# silver.orders_daily_kpis (alias: sodk)
df_sodk = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_daily_kpis.{FILE_FORMAT}/")
)
df_sodk.createOrReplaceTempView("orders_daily_kpis")

# ===================================================================================
# TARGET: gold.customer_orders
# - Select directly from silver.customer_orders
# - Apply exact mappings/casts using Spark SQL functions
# - Write SINGLE CSV file: TARGET_PATH + "/customer_orders.csv"
# ===================================================================================
df_customer_orders = spark.sql("""
SELECT
  CAST(sco.order_id AS STRING)                     AS order_id,
  DATE(sco.order_date)                             AS order_date,
  CAST(sco.customer_id AS STRING)                  AS customer_id,
  CAST(sco.order_status AS STRING)                 AS order_status,
  CAST(sco.order_total_amount AS DECIMAL(38, 10))  AS order_total_amount,
  CAST(sco.currency_code AS STRING)                AS currency_code,
  DATE(sco.ingestion_date)                         AS ingestion_date
FROM customer_orders sco
""")

tmp_customer_orders_path = f"{TARGET_PATH}/__tmp_customer_orders_csv_write"
(
    df_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(tmp_customer_orders_path)
)

tmp_customer_orders_file = (
    spark.read.format("binaryFile")
    .load(f"{tmp_customer_orders_path}/part-*.csv")
    .select("path")
    .head()[0]
)

# Move the single part file to the required exact path: s3://.../customer_orders.csv
jvm = spark._jvm
hconf = sc._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)

src_path = jvm.org.apache.hadoop.fs.Path(tmp_customer_orders_file)
dst_path = jvm.org.apache.hadoop.fs.Path(f"{TARGET_PATH}/customer_orders.csv")

# Clean destination if exists, then rename
if fs.exists(dst_path):
    fs.delete(dst_path, True)
fs.rename(src_path, dst_path)

# Cleanup temp folder
fs.delete(jvm.org.apache.hadoop.fs.Path(tmp_customer_orders_path), True)

# ===================================================================================
# TARGET: gold.customer_order_items
# - Select directly from silver.customer_order_items
# - Apply exact mappings/casts using Spark SQL functions
# - Write SINGLE CSV file: TARGET_PATH + "/customer_order_items.csv"
# ===================================================================================
df_customer_order_items = spark.sql("""
SELECT
  CAST(scoi.order_id AS STRING)                    AS order_id,
  CAST(scoi.order_item_id AS INT)                  AS order_item_id,
  CAST(scoi.product_id AS STRING)                  AS product_id,
  CAST(scoi.quantity AS INT)                       AS quantity,
  CAST(scoi.unit_price AS DECIMAL(38, 10))         AS unit_price,
  CAST(scoi.line_total_amount AS DECIMAL(38, 10))  AS line_total_amount
FROM customer_order_items scoi
""")

tmp_customer_order_items_path = f"{TARGET_PATH}/__tmp_customer_order_items_csv_write"
(
    df_customer_order_items.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(tmp_customer_order_items_path)
)

tmp_customer_order_items_file = (
    spark.read.format("binaryFile")
    .load(f"{tmp_customer_order_items_path}/part-*.csv")
    .select("path")
    .head()[0]
)

src_path = jvm.org.apache.hadoop.fs.Path(tmp_customer_order_items_file)
dst_path = jvm.org.apache.hadoop.fs.Path(f"{TARGET_PATH}/customer_order_items.csv")

if fs.exists(dst_path):
    fs.delete(dst_path, True)
fs.rename(src_path, dst_path)

fs.delete(jvm.org.apache.hadoop.fs.Path(tmp_customer_order_items_path), True)

# ===================================================================================
# TARGET: gold.orders_daily_kpis
# - Select directly from silver.orders_daily_kpis
# - Apply exact mappings/casts using Spark SQL functions
# - Write SINGLE CSV file: TARGET_PATH + "/orders_daily_kpis.csv"
# ===================================================================================
df_orders_daily_kpis = spark.sql("""
SELECT
  DATE(sodk.kpi_date)                              AS kpi_date,
  CAST(sodk.total_orders AS INT)                   AS total_orders,
  CAST(sodk.total_order_amount AS DECIMAL(38, 10)) AS total_order_amount,
  CAST(sodk.validated_orders AS INT)               AS validated_orders,
  CAST(sodk.rejected_orders AS INT)                AS rejected_orders
FROM orders_daily_kpis sodk
""")

tmp_orders_daily_kpis_path = f"{TARGET_PATH}/__tmp_orders_daily_kpis_csv_write"
(
    df_orders_daily_kpis.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(tmp_orders_daily_kpis_path)
)

tmp_orders_daily_kpis_file = (
    spark.read.format("binaryFile")
    .load(f"{tmp_orders_daily_kpis_path}/part-*.csv")
    .select("path")
    .head()[0]
)

src_path = jvm.org.apache.hadoop.fs.Path(tmp_orders_daily_kpis_file)
dst_path = jvm.org.apache.hadoop.fs.Path(f"{TARGET_PATH}/orders_daily_kpis.csv")

if fs.exists(dst_path):
    fs.delete(dst_path, True)
fs.rename(src_path, dst_path)

fs.delete(jvm.org.apache.hadoop.fs.Path(tmp_orders_daily_kpis_path), True)

job.commit()
```