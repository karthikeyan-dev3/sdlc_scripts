import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------------------------------------
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------------------
orders_silver_df.createOrReplaceTempView("orders_silver")
customers_silver_df.createOrReplaceTempView("customers_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# --------------------------------------------------------------------------------------------------
# TARGET TABLE: gold_orders
# Mapping: silver.orders_silver os LEFT JOIN silver.customers_silver cs ON os.customer_id = cs.customer_id
# --------------------------------------------------------------------------------------------------
gold_orders_df = spark.sql("""
SELECT
    CAST(os.order_id AS STRING)                              AS order_id,
    CAST(os.order_date AS DATE)                              AS order_date,
    CAST(os.order_timestamp AS TIMESTAMP)                    AS order_timestamp,
    CAST(os.customer_id AS STRING)                           AS customer_id,
    CAST(os.order_total_amount AS DECIMAL(38,10))            AS order_total_amount,
    CAST(os.source_file_date AS DATE)                        AS source_file_date,
    CAST(os.ingested_at AS TIMESTAMP)                        AS ingested_at,
    CAST(os.loaded_at AS TIMESTAMP)                          AS loaded_at,

    CAST(NULL AS STRING)                                     AS currency_code,
    CAST(NULL AS STRING)                                     AS payment_method,
    CAST(NULL AS STRING)                                     AS shipping_method,
    CAST(NULL AS STRING)                                     AS shipping_city,
    CAST(NULL AS STRING)                                     AS shipping_state,
    CAST(NULL AS STRING)                                     AS shipping_postal_code,
    CAST(NULL AS STRING)                                     AS shipping_country
FROM orders_silver os
LEFT JOIN customers_silver cs
    ON os.customer_id = cs.customer_id
""")

(
    gold_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_orders.csv")
)

# --------------------------------------------------------------------------------------------------
# TARGET TABLE: gold_order_items
# Mapping: silver.order_items_silver ois INNER JOIN silver.orders_silver os ON ois.order_id = os.order_id
# --------------------------------------------------------------------------------------------------
gold_order_items_df = spark.sql("""
SELECT
    CAST(ois.order_item_id AS STRING)                        AS order_item_id,
    CAST(ois.order_id AS STRING)                             AS order_id,
    CAST(ois.product_id AS STRING)                           AS product_id,
    CAST(ois.quantity AS INT)                                AS quantity,
    CAST(ois.unit_price AS DECIMAL(38,10))                   AS unit_price,
    CAST(ois.line_amount AS DECIMAL(38,10))                  AS line_amount
FROM order_items_silver ois
INNER JOIN orders_silver os
    ON ois.order_id = os.order_id
""")

(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

# --------------------------------------------------------------------------------------------------
# TARGET TABLE: gold_customers
# Mapping: silver.customers_silver cs
# --------------------------------------------------------------------------------------------------
gold_customers_df = spark.sql("""
SELECT
    CAST(cs.customer_id AS STRING)                           AS customer_id,
    CAST(cs.customer_created_date AS DATE)                   AS customer_created_date,
    CAST(NULL AS STRING)                                     AS customer_name,
    CAST(NULL AS STRING)                                     AS customer_email,
    CAST(NULL AS STRING)                                     AS customer_phone
FROM customers_silver cs
""")

(
    gold_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customers.csv")
)

# --------------------------------------------------------------------------------------------------
# TARGET TABLE: gold_products
# Mapping: silver.products_silver ps
# --------------------------------------------------------------------------------------------------
gold_products_df = spark.sql("""
SELECT
    CAST(ps.product_id AS STRING)                            AS product_id,
    CAST(ps.product_name AS STRING)                          AS product_name,
    CAST(ps.product_category AS STRING)                      AS product_category
FROM products_silver ps
""")

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)