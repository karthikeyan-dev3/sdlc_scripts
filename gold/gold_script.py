```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Parameters / Constants
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Glue / Spark init
# ------------------------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Read Source Tables from S3 (Silver)
# NOTE: Per requirement, ALWAYS use .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")

# ====================================================================================
# Target Table: gold.gold_customer_orders
# Source: silver.customer_orders_silver cos
# Transformations (EXACT from UDT):
#   gco.order_id         = cos.order_id
#   gco.order_date       = cos.order_date
#   gco.order_timestamp  = cos.order_timestamp
#   gco.total_amount     = cos.total_amount
# ====================================================================================
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)             AS order_id,
        CAST(cos.order_date AS DATE)             AS order_date,
        CAST(cos.order_timestamp AS TIMESTAMP)   AS order_timestamp,
        CAST(cos.total_amount AS DECIMAL(38, 18)) AS total_amount
    FROM customer_orders_silver cos
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ====================================================================================
# Target Table: gold.gold_order_items
# Source: silver.order_items_silver ois
# Join: INNER JOIN silver.customer_orders_silver cos ON ois.order_id = cos.order_id
# Transformations (EXACT from UDT):
#   goi.order_id       = ois.order_id
#   goi.order_item_id  = ois.order_item_id
#   goi.product_id     = ois.product_id
#   goi.quantity       = ois.quantity
# ====================================================================================
gold_order_items_df = spark.sql(
    """
    SELECT
        CAST(ois.order_id AS STRING)        AS order_id,
        CAST(ois.order_item_id AS STRING)   AS order_item_id,
        CAST(ois.product_id AS STRING)      AS product_id,
        CAST(ois.quantity AS INT)           AS quantity
    FROM order_items_silver ois
    INNER JOIN customer_orders_silver cos
        ON ois.order_id = cos.order_id
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    gold_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_items.csv")
)

job.commit()
```