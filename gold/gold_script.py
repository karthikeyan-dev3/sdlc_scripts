```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
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

# Recommended CSV options for consistent reads/writes
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "quote": '"',
    "escape": '"',
    "multiLine": "false",
}
CSV_WRITE_OPTIONS = {
    "header": "true",
    "quote": '"',
    "escape": '"',
}

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    SOURCE READING RULE:
#    .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
silver_customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_customer_orders.{FILE_FORMAT}/")
)

silver_order_items_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_order_items.{FILE_FORMAT}/")
)

silver_customers_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_customers.{FILE_FORMAT}/")
)

silver_products_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_products.{FILE_FORMAT}/")
)

silver_daily_order_kpis_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_daily_order_kpis.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
silver_customer_orders_df.createOrReplaceTempView("silver_customer_orders")
silver_order_items_df.createOrReplaceTempView("silver_order_items")
silver_customers_df.createOrReplaceTempView("silver_customers")
silver_products_df.createOrReplaceTempView("silver_products")
silver_daily_order_kpis_df.createOrReplaceTempView("silver_daily_order_kpis")

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_customer_orders  (alias: gco)
# Source: silver.silver_customer_orders sco
# Transformations: EXACT mapping (pass-through) with explicit CAST/DATE where applicable
# -----------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(sco.order_id AS STRING)                              AS order_id,
        DATE(sco.order_date)                                      AS order_date,
        CAST(sco.customer_id AS STRING)                           AS customer_id,
        CAST(sco.order_status AS STRING)                          AS order_status,
        CAST(sco.currency_code AS STRING)                         AS currency_code,
        CAST(sco.order_subtotal_amount AS DECIMAL(18,2))          AS order_subtotal_amount,
        CAST(sco.order_tax_amount AS DECIMAL(18,2))               AS order_tax_amount,
        CAST(sco.order_shipping_amount AS DECIMAL(18,2))          AS order_shipping_amount,
        CAST(sco.order_discount_amount AS DECIMAL(18,2))          AS order_discount_amount,
        CAST(sco.order_total_amount AS DECIMAL(18,2))             AS order_total_amount
    FROM silver_customer_orders sco
    """
)

# Write as a SINGLE CSV file directly under TARGET_PATH
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_order_items  (alias: goi)
# Source: silver.silver_order_items soi
# Transformations: EXACT mapping (pass-through) with explicit CAST
# -----------------------------------------------------------------------------------
gold_order_items_df = spark.sql(
    """
    SELECT
        CAST(soi.order_id AS STRING)                      AS order_id,
        CAST(soi.order_item_id AS STRING)                 AS order_item_id,
        CAST(soi.product_id AS STRING)                    AS product_id,
        CAST(soi.quantity AS INT)                         AS quantity,
        CAST(soi.unit_price_amount AS DECIMAL(18,2))      AS unit_price_amount,
        CAST(soi.line_discount_amount AS DECIMAL(18,2))   AS line_discount_amount,
        CAST(soi.line_total_amount AS DECIMAL(18,2))      AS line_total_amount
    FROM silver_order_items soi
    """
)

(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_customers  (alias: gc)
# Source: silver.silver_customers sc
# Transformations: EXACT mapping (pass-through) with explicit CAST/DATE
# -----------------------------------------------------------------------------------
gold_customers_df = spark.sql(
    """
    SELECT
        CAST(sc.customer_id AS STRING)     AS customer_id,
        CAST(sc.customer_name AS STRING)   AS customer_name,
        CAST(sc.customer_email AS STRING)  AS customer_email,
        DATE(sc.customer_created_date)     AS customer_created_date
    FROM silver_customers sc
    """
)

(
    gold_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_customers.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_products  (alias: gp)
# Source: silver.silver_products sp
# Transformations: EXACT mapping (pass-through) with explicit CAST
# -----------------------------------------------------------------------------------
gold_products_df = spark.sql(
    """
    SELECT
        CAST(sp.product_id AS STRING)         AS product_id,
        CAST(sp.product_name AS STRING)       AS product_name,
        CAST(sp.product_category AS STRING)   AS product_category
    FROM silver_products sp
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_daily_order_kpis  (alias: gdk)
# Source: silver.silver_daily_order_kpis sdk
# Transformations: EXACT mapping (pass-through) with explicit CAST/DATE
# -----------------------------------------------------------------------------------
gold_daily_order_kpis_df = spark.sql(
    """
    SELECT
        DATE(sdk.order_date)                                AS order_date,
        CAST(sdk.orders_count AS INT)                       AS orders_count,
        CAST(sdk.unique_customers_count AS INT)             AS unique_customers_count,
        CAST(sdk.gross_sales_amount AS DECIMAL(18,2))       AS gross_sales_amount,
        CAST(sdk.discount_amount AS DECIMAL(18,2))          AS discount_amount,
        CAST(sdk.tax_amount AS DECIMAL(18,2))               AS tax_amount,
        CAST(sdk.shipping_amount AS DECIMAL(18,2))          AS shipping_amount,
        CAST(sdk.net_sales_amount AS DECIMAL(18,2))         AS net_sales_amount
    FROM silver_daily_order_kpis sdk
    """
)

(
    gold_daily_order_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_daily_order_kpis.csv")
)

job.commit()
```