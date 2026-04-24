```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
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

# ===================================================================================
# SOURCE READS + TEMP VIEWS
# ===================================================================================

# silver.customer_orders_silver cos
cos_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
cos_df.createOrReplaceTempView("customer_orders_silver")

# silver.customer_order_items_silver cois
cois_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
cois_df.createOrReplaceTempView("customer_order_items_silver")

# silver.customers_silver cslv
cslv_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)
cslv_df.createOrReplaceTempView("customers_silver")

# silver.products_silver ps
ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("products_silver")

# silver.daily_order_kpis_silver doks
doks_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/daily_order_kpis_silver.{FILE_FORMAT}/")
)
doks_df.createOrReplaceTempView("daily_order_kpis_silver")

# ===================================================================================
# TARGET: gold.gold_customer_orders  (gco)
# Source: silver.customer_orders_silver (cos)
# ===================================================================================
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)                       AS order_id,
        CAST(cos.order_date AS DATE)                       AS order_date,
        CAST(cos.customer_id AS STRING)                    AS customer_id,
        CAST(cos.order_status AS STRING)                   AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 10))    AS order_total_amount,
        CAST(cos.currency_code AS STRING)                  AS currency_code,
        CAST(cos.source_system AS STRING)                  AS source_system,
        CAST(cos.ingestion_date AS DATE)                   AS ingestion_date
    FROM customer_orders_silver cos
    """
)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET: gold.gold_customer_order_items  (gcoi)
# Source: silver.customer_order_items_silver (cois)
# ===================================================================================
gold_customer_order_items_df = spark.sql(
    """
    SELECT
        CAST(cois.order_id AS STRING)                      AS order_id,
        CAST(cois.order_item_id AS STRING)                 AS order_item_id,
        CAST(cois.product_id AS STRING)                    AS product_id,
        CAST(cois.quantity AS INT)                         AS quantity,
        CAST(cois.unit_price AS DECIMAL(38, 10))           AS unit_price,
        CAST(cois.line_amount AS DECIMAL(38, 10))          AS line_amount,
        CAST(cois.ingestion_date AS DATE)                  AS ingestion_date
    FROM customer_order_items_silver cois
    """
)

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# TARGET: gold.gold_customers  (gcust)
# Source: silver.customers_silver (cslv)
# ===================================================================================
gold_customers_df = spark.sql(
    """
    SELECT
        CAST(cslv.customer_id AS STRING)           AS customer_id,
        CAST(cslv.customer_name AS STRING)         AS customer_name,
        CAST(cslv.customer_email AS STRING)        AS customer_email,
        CAST(cslv.customer_phone AS STRING)        AS customer_phone,
        CAST(cslv.billing_country AS STRING)       AS billing_country,
        CAST(cslv.shipping_country AS STRING)      AS shipping_country,
        CAST(cslv.is_active AS BOOLEAN)            AS is_active,
        CAST(cslv.effective_start_date AS DATE)    AS effective_start_date,
        CAST(cslv.effective_end_date AS DATE)      AS effective_end_date
    FROM customers_silver cslv
    """
)

(
    gold_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customers.csv")
)

# ===================================================================================
# TARGET: gold.gold_products  (gp)
# Source: silver.products_silver (ps)
# ===================================================================================
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)              AS product_id,
        CAST(ps.product_name AS STRING)            AS product_name,
        CAST(ps.product_category AS STRING)        AS product_category,
        CAST(ps.is_active AS BOOLEAN)              AS is_active,
        CAST(ps.effective_start_date AS DATE)      AS effective_start_date,
        CAST(ps.effective_end_date AS DATE)        AS effective_end_date
    FROM products_silver ps
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# ===================================================================================
# TARGET: gold.gold_daily_order_kpis  (gkpi)
# Source: silver.daily_order_kpis_silver (doks)
# ===================================================================================
gold_daily_order_kpis_df = spark.sql(
    """
    SELECT
        CAST(doks.kpi_date AS DATE)                        AS kpi_date,
        CAST(doks.orders_count AS INT)                     AS orders_count,
        CAST(doks.items_count AS INT)                      AS items_count,
        CAST(doks.gross_sales_amount AS DECIMAL(38, 10))   AS gross_sales_amount,
        CAST(doks.avg_order_value AS DECIMAL(38, 10))      AS avg_order_value,
        CAST(doks.distinct_customers_count AS INT)         AS distinct_customers_count,
        CAST(doks.ingestion_date AS DATE)                  AS ingestion_date
    FROM daily_order_kpis_silver doks
    """
)

(
    gold_daily_order_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_order_kpis.csv")
)

job.commit()
```