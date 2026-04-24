```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -----------------------------------------------------------------------------------
# Glue job setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ===================================================================================
# SOURCE READS + TEMP VIEWS
# ===================================================================================

# --- silver.customer_orders_silver (cos)
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# --- silver.currencies_silver (cur)
df_currencies_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/currencies_silver.{FILE_FORMAT}/")
)
df_currencies_silver.createOrReplaceTempView("currencies_silver")

# --- silver.customer_order_items_silver (cois)
df_customer_order_items_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
df_customer_order_items_silver.createOrReplaceTempView("customer_order_items_silver")

# ===================================================================================
# TARGET TABLE: gold.gold_customer_orders
# One row per order_id. Join to default currency row to backfill currency_code.
# ===================================================================================
df_gold_customer_orders = spark.sql("""
SELECT
    CAST(cos.order_id AS STRING)                              AS order_id,
    DATE(cos.order_date)                                      AS order_date,
    CAST(cos.customer_id AS STRING)                           AS customer_id,
    CAST(cos.order_status AS STRING)                          AS order_status,
    CAST(cos.order_total_amount AS DECIMAL(38, 18))           AS order_total_amount,
    CAST(COALESCE(cos.currency_code, cur.currency_code) AS STRING) AS currency_code
FROM customer_orders_silver cos
LEFT JOIN currencies_silver cur
    ON cur.is_default = TRUE
""")

(
    df_gold_customer_orders
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_customer_order_items
# One row per order_item_id sourced directly from silver.customer_order_items_silver.
# ===================================================================================
df_gold_customer_order_items = spark.sql("""
SELECT
    CAST(cois.order_id AS STRING)                   AS order_id,
    CAST(cois.order_item_id AS STRING)              AS order_item_id,
    CAST(cois.product_id AS STRING)                 AS product_id,
    CAST(cois.quantity AS INT)                      AS quantity,
    CAST(cois.unit_price AS DECIMAL(38, 18))        AS unit_price,
    CAST(cois.line_total_amount AS DECIMAL(38, 18)) AS line_total_amount
FROM customer_order_items_silver cois
""")

(
    df_gold_customer_order_items
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

job.commit()
```