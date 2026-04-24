```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark setup
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
# Source Reads (Silver)
# ====================================================================================
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

# Temp views
orders_silver_df.createOrReplaceTempView("orders_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# ====================================================================================
# Target: gold.gold_customer_orders
# Mapping: silver.orders_silver os
# ====================================================================================
gold_customer_orders_sql = """
WITH base AS (
  SELECT
    CAST(os.order_id AS STRING)                             AS order_id,
    CAST(os.order_date AS DATE)                             AS order_date,
    CAST(os.customer_id AS STRING)                          AS customer_id,
    CAST(os.customer_name AS STRING)                        AS customer_name,
    CAST(os.customer_email AS STRING)                       AS customer_email,
    CAST(os.customer_phone AS STRING)                       AS customer_phone,
    CAST(os.shipping_address_line1 AS STRING)               AS shipping_address_line1,
    CAST(os.shipping_address_line2 AS STRING)               AS shipping_address_line2,
    CAST(os.shipping_city AS STRING)                        AS shipping_city,
    CAST(os.shipping_state AS STRING)                       AS shipping_state,
    CAST(os.shipping_postal_code AS STRING)                 AS shipping_postal_code,
    CAST(os.shipping_country AS STRING)                     AS shipping_country,
    CAST(os.order_status AS STRING)                         AS order_status,
    CAST(os.order_total_amount AS DECIMAL(38, 10))           AS order_total_amount,
    CAST(os.currency_code AS STRING)                        AS currency_code,
    CAST(os.payment_method AS STRING)                       AS payment_method,
    CAST(os.source_system AS STRING)                        AS source_system,
    CAST(os.ingestion_date AS DATE)                         AS ingestion_date,
    CAST(os.record_created_ts AS TIMESTAMP)                 AS record_created_ts,
    CAST(os.record_updated_ts AS TIMESTAMP)                 AS record_updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY os.order_id
      ORDER BY CAST(os.record_updated_ts AS TIMESTAMP) DESC, CAST(os.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM orders_silver os
)
SELECT
  order_id,
  order_date,
  customer_id,
  customer_name,
  customer_email,
  customer_phone,
  shipping_address_line1,
  shipping_address_line2,
  shipping_city,
  shipping_state,
  shipping_postal_code,
  shipping_country,
  order_status,
  order_total_amount,
  currency_code,
  payment_method,
  source_system,
  ingestion_date,
  record_created_ts,
  record_updated_ts
FROM base
WHERE rn = 1
"""

gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

gold_customer_orders_output_path = f"{TARGET_PATH}/gold_customer_orders.csv"
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_output_path)
)

# ====================================================================================
# Target: gold.gold_customer_order_items
# Mapping: silver.order_items_silver ois INNER JOIN silver.orders_silver os ON ois.order_id = os.order_id
# ====================================================================================
gold_customer_order_items_sql = """
WITH joined AS (
  SELECT
    CAST(ois.order_id AS STRING)                            AS order_id,
    CAST(ois.order_item_id AS STRING)                       AS order_item_id,
    CAST(ois.product_id AS STRING)                          AS product_id,
    CAST(ois.product_name AS STRING)                        AS product_name,
    CAST(ois.product_category AS STRING)                    AS product_category,
    CAST(ois.quantity AS INT)                               AS quantity,
    CAST(ois.unit_price AS DECIMAL(38, 10))                 AS unit_price,
    CAST(ois.item_discount_amount AS DECIMAL(38, 10))       AS item_discount_amount,
    CAST(ois.item_tax_amount AS DECIMAL(38, 10))            AS item_tax_amount,
    CAST(ois.item_total_amount AS DECIMAL(38, 10))          AS item_total_amount,
    CAST(os.currency_code AS STRING)                        AS currency_code,
    CAST(os.ingestion_date AS DATE)                         AS ingestion_date,
    CAST(ois.record_created_ts AS TIMESTAMP)                AS record_created_ts,
    CAST(ois.record_updated_ts AS TIMESTAMP)                AS record_updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY ois.order_id, ois.order_item_id
      ORDER BY CAST(ois.record_updated_ts AS TIMESTAMP) DESC, CAST(ois.record_created_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM order_items_silver ois
  INNER JOIN orders_silver os
    ON ois.order_id = os.order_id
)
SELECT
  order_id,
  order_item_id,
  product_id,
  product_name,
  product_category,
  quantity,
  unit_price,
  item_discount_amount,
  item_tax_amount,
  item_total_amount,
  currency_code,
  ingestion_date,
  record_created_ts,
  record_updated_ts
FROM joined
WHERE rn = 1
"""

gold_customer_order_items_df = spark.sql(gold_customer_order_items_sql)

gold_customer_order_items_output_path = f"{TARGET_PATH}/gold_customer_order_items.csv"
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_order_items_output_path)
)

job.commit()
```