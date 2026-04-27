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
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read source tables from S3 (STRICT PATH FORMAT)
# -----------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

shipping_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/shipping_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create temp views
# -----------------------------------------------------------------------------------
orders_bronze_df.createOrReplaceTempView("orders_bronze")
shipping_bronze_df.createOrReplaceTempView("shipping_bronze")
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# ===================================================================================
# TABLE: silver.customer_orders_silver
# 1) Transform with Spark SQL (apply EXACT transformations + ROW_NUMBER dedup)
# 2) Write as SINGLE CSV file directly under TARGET_PATH
# ===================================================================================
customer_orders_silver_sql = """
WITH base AS (
    SELECT
        -- UDT mappings (EXACT)
        ob.transaction_id                                      AS order_id,
        CAST(ob.transaction_time AS DATE)                      AS order_date,
        ob.sale_amount                                         AS order_total_amount,
        CASE
            WHEN sb.state IS NOT NULL OR sb.city IS NOT NULL THEN 'US'
            ELSE 'US'
        END                                                    AS shipping_country_code,

        -- Defaults / constants from table description
        COALESCE(ob.order_status, 'COMPLETED')                 AS order_status,
        COALESCE(ob.currency_code, 'USD')                      AS currency_code,
        'UNKNOWN'                                              AS payment_method,
        'UNKNOWN'                                              AS customer_id,
        'sales_transactions_raw'                               AS source_system,
        CAST(current_date() AS DATE)                           AS ingestion_date,

        -- Dedup key
        ROW_NUMBER() OVER (
            PARTITION BY ob.transaction_id
            ORDER BY ob.transaction_time DESC
        )                                                      AS rn
    FROM orders_bronze ob
    LEFT JOIN shipping_bronze sb
        ON ob.store_id = sb.store_id
)
SELECT
    order_id,
    order_date,
    order_total_amount,
    shipping_country_code,
    order_status,
    currency_code,
    payment_method,
    customer_id,
    source_system,
    ingestion_date
FROM base
WHERE rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ===================================================================================
# TABLE: silver.customer_order_items_silver
# 1) Transform with Spark SQL (apply EXACT transformations + ROW_NUMBER dedup)
# 2) Write as SINGLE CSV file directly under TARGET_PATH
# ===================================================================================
customer_order_items_silver_sql = """
WITH base AS (
    SELECT
        -- UDT mappings (EXACT)
        oib.transaction_id                                     AS order_id,
        oib.product_id                                         AS product_id,
        COALESCE(oib.quantity, 1)                              AS quantity,
        pb.price                                               AS unit_price_amount,
        COALESCE(oib.quantity, 1) * pb.price                   AS line_total_amount,

        -- Defaults / constants from table description
        COALESCE(oib.currency_code, 'USD')                     AS currency_code,
        CAST(current_date() AS DATE)                           AS ingestion_date,

        -- Dedup + deterministic order_item_id
        ROW_NUMBER() OVER (
            PARTITION BY oib.transaction_id, oib.product_id
            ORDER BY oib.transaction_id, oib.product_id
        )                                                      AS rn
    FROM order_items_bronze oib
    LEFT JOIN products_bronze pb
        ON oib.product_id = pb.product_id
),
dedup AS (
    SELECT
        order_id,
        product_id,
        quantity,
        unit_price_amount,
        line_total_amount,
        currency_code,
        ingestion_date
    FROM base
    WHERE rn = 1
)
SELECT
    -- deterministic order_item_id derived from deduped set
    ROW_NUMBER() OVER (ORDER BY order_id, product_id)          AS order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_amount,
    line_total_amount,
    currency_code,
    ingestion_date
FROM dedup
"""

customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

job.commit()
```