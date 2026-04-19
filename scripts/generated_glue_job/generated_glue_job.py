```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

VALID_ORDER_STATUSES = ("COMPLETED", "PAID", "FULFILLED")

# -----------------------------------------------------------------------------------
# Read sources from S3 (Bronze) and create temp views
# NOTE: Path format is строго: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)
customers_bronze_df.createOrReplaceTempView("customers_bronze")

campaign_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/campaign_events_bronze.{FILE_FORMAT}/")
)
campaign_events_bronze_df.createOrReplaceTempView("campaign_events_bronze")

# ===================================================================================
# Target: silver.order_line_sales_silver
# Sources: order_items_bronze oib
#          INNER JOIN orders_bronze ob ON oib.transaction_id = ob.transaction_id
#          INNER JOIN products_bronze pb ON oib.product_id = pb.product_id
#          LEFT JOIN stores_bronze sb ON ob.store_id = sb.store_id
# Transformations:
#   sales_date        = DATE(ob.transaction_datetime)
#   line_revenue      = COALESCE(oib.total_price, oib.quantity * oib.unit_price)
#   units_sold        = oib.quantity
# Dedup:
#   one row per (transaction_item_id) via ROW_NUMBER
# Filter:
#   valid sales statuses
# ===================================================================================
order_line_sales_silver_sql = f"""
WITH base AS (
  SELECT
    CAST(DATE(ob.transaction_datetime) AS DATE)                                        AS sales_date,
    CAST(oib.transaction_item_id AS STRING)                                            AS transaction_item_id,
    CAST(oib.transaction_id AS STRING)                                                 AS transaction_id,
    CAST(ob.customer_id AS STRING)                                                     AS customer_id,
    CAST(ob.store_id AS STRING)                                                        AS store_id,
    CAST(sb.store_name AS STRING)                                                      AS store_name,
    CAST(sb.region AS STRING)                                                          AS region_name,
    CAST(oib.product_id AS STRING)                                                     AS sku_id,
    CAST(pb.product_name AS STRING)                                                    AS sku_name,
    CAST(pb.category AS STRING)                                                        AS category_name,
    CAST(oib.quantity AS INT)                                                          AS units_sold,
    CAST(COALESCE(oib.total_price, oib.quantity * oib.unit_price) AS DECIMAL(12,2))    AS line_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY oib.transaction_item_id
      ORDER BY ob.transaction_datetime DESC
    ) AS rn
  FROM order_items_bronze oib
  INNER JOIN orders_bronze ob
    ON oib.transaction_id = ob.transaction_id
  INNER JOIN products_bronze pb
    ON oib.product_id = pb.product_id
  LEFT JOIN stores_bronze sb
    ON ob.store_id = sb.store_id
  WHERE UPPER(TRIM(COALESCE(ob.status, ''))) IN {VALID_ORDER_STATUSES}
)
SELECT
  sales_date,
  transaction_item_id,
  transaction_id,
  customer_id,
  store_id,
  store_name,
  region_name,
  sku_id,
  sku_name,
  category_name,
  units_sold,
  line_revenue
FROM base
WHERE rn = 1
"""
order_line_sales_silver_df = spark.sql(order_line_sales_silver_sql)

(
    order_line_sales_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_line_sales_silver.csv")
)

# ===================================================================================
# Target: silver.customer_order_first_purchase_silver
# Sources: orders_bronze ob
#          LEFT JOIN customers_bronze cb ON ob.customer_id = cb.customer_id
# Transformations:
#   order_date          = DATE(ob.transaction_datetime)
#   first_purchase_date = MIN(DATE(ob.transaction_datetime)) OVER (PARTITION BY ob.customer_id)
# Dedup:
#   one row per (transaction_id) via ROW_NUMBER
# Filter:
#   valid sales statuses
# ===================================================================================
customer_order_first_purchase_silver_sql = f"""
WITH base AS (
  SELECT
    CAST(ob.transaction_id AS STRING) AS transaction_id,
    CAST(ob.customer_id AS STRING)    AS customer_id,
    CAST(DATE(ob.transaction_datetime) AS DATE) AS order_date,
    CAST(
      MIN(DATE(ob.transaction_datetime)) OVER (PARTITION BY ob.customer_id)
      AS DATE
    ) AS first_purchase_date,
    ROW_NUMBER() OVER (
      PARTITION BY ob.transaction_id
      ORDER BY ob.transaction_datetime DESC
    ) AS rn
  FROM orders_bronze ob
  LEFT JOIN customers_bronze cb
    ON ob.customer_id = cb.customer_id
  WHERE UPPER(TRIM(COALESCE(ob.status, ''))) IN {VALID_ORDER_STATUSES}
)
SELECT
  transaction_id,
  customer_id,
  order_date,
  first_purchase_date
FROM base
WHERE rn = 1
"""
customer_order_first_purchase_silver_df = spark.sql(customer_order_first_purchase_silver_sql)

(
    customer_order_first_purchase_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_first_purchase_silver.csv")
)

# ===================================================================================
# Target: silver.daily_campaign_attribution_silver
# Sources: campaign_events_bronze ceb
#          LEFT JOIN orders_bronze ob
#            ON ceb.customer_id = ob.customer_id
#           AND DATE(ob.transaction_datetime) = DATE(ceb.event_timestamp)
# Transformations:
#   campaign_date       = DATE(ceb.event_timestamp)
#   conversions         = SUM(CASE WHEN ceb.conversion_flag = true OR ceb.event_type='conversion' THEN 1 ELSE 0 END)
#   attributed_orders   = COUNT(DISTINCT ob.transaction_id)  (valid sales statuses)
#   attributed_revenue  = SUM(ob.total_amount)               (valid sales statuses)
# Dedup:
#   events to one row per (event_id) via ROW_NUMBER
# ===================================================================================
daily_campaign_attribution_silver_sql = f"""
WITH events_dedup AS (
  SELECT
    CAST(ceb.event_id AS STRING)          AS event_id,
    CAST(ceb.campaign_id AS STRING)       AS campaign_id,
    CAST(DATE(ceb.event_timestamp) AS DATE) AS campaign_date,
    CAST(ceb.event_type AS STRING)        AS event_type,
    CAST(ceb.conversion_flag AS BOOLEAN)  AS conversion_flag,
    CAST(ceb.customer_id AS STRING)       AS customer_id,
    ROW_NUMBER() OVER (
      PARTITION BY ceb.event_id
      ORDER BY ceb.event_timestamp DESC
    ) AS rn
  FROM campaign_events_bronze ceb
),
events_clean AS (
  SELECT
    event_id,
    campaign_id,
    campaign_date,
    event_type,
    conversion_flag,
    customer_id
  FROM events_dedup
  WHERE rn = 1
),
events_join_orders AS (
  SELECT
    e.campaign_date,
    e.campaign_id,
    e.event_id,
    e.event_type,
    e.conversion_flag,
    ob.transaction_id,
    ob.total_amount,
    ob.status
  FROM events_clean e
  LEFT JOIN orders_bronze ob
    ON e.customer_id = ob.customer_id
   AND DATE(ob.transaction_datetime) = e.campaign_date
),
final AS (
  SELECT
    CAST(campaign_date AS DATE) AS campaign_date,
    CAST(campaign_id AS STRING) AS campaign_id,

    -- Event-level columns (from UDT columns list)
    CAST(event_id AS STRING)    AS event_id,
    CAST(event_type AS STRING)  AS event_type,
    CAST(conversion_flag AS BOOLEAN) AS conversion_flag,

    -- Attribution metrics (from UDT columns list)
    CAST(
      COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(COALESCE(status, ''))) IN {VALID_ORDER_STATUSES}
        THEN transaction_id
        ELSE NULL
      END) AS INT
    ) AS attributed_orders,

    CAST(
      COALESCE(SUM(CASE
        WHEN UPPER(TRIM(COALESCE(status, ''))) IN {VALID_ORDER_STATUSES}
        THEN total_amount
        ELSE NULL
      END), 0) AS DECIMAL(12,2)
    ) AS attributed_revenue
  FROM events_join_orders
  GROUP BY
    campaign_date, campaign_id, event_id, event_type, conversion_flag
)
SELECT
  campaign_date,
  campaign_id,
  event_id,
  event_type,
  conversion_flag,
  attributed_orders,
  attributed_revenue
FROM final
"""
daily_campaign_attribution_silver_df = spark.sql(daily_campaign_attribution_silver_sql)

(
    daily_campaign_attribution_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_campaign_attribution_silver.csv")
)

job.commit()
```