```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark session
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source: bronze.customer_orders
# ------------------------------------------------------------------------------------
bronze_customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
bronze_customer_orders_df.createOrReplaceTempView("customer_orders")

# ====================================================================================
# Target Table: silver.customer_orders
# - Dedup using ROW_NUMBER over transaction_id ordered by transaction_time desc
# - Conform mapping:
#   order_id      = transaction_id
#   order_date    = CAST(transaction_time AS DATE)
#   customer_id   = store_id
#   order_amount  = sale_amount
#   order_status  = VALID/INVALID rule from UDT
# ====================================================================================
silver_customer_orders_sql = """
WITH ranked AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.transaction_id
      ORDER BY co.transaction_time DESC
    ) AS rn
  FROM customer_orders co
)
SELECT
  CAST(r.transaction_id AS STRING) AS order_id,
  CAST(r.transaction_time AS DATE) AS order_date,
  CAST(r.store_id AS STRING) AS customer_id,
  CAST(r.sale_amount AS DECIMAL(38,10)) AS order_amount,
  CASE
    WHEN r.transaction_id IS NOT NULL
      AND r.store_id IS NOT NULL
      AND r.sale_amount IS NOT NULL
      AND r.sale_amount >= 0
      AND r.quantity IS NOT NULL
      AND r.quantity > 0
    THEN 'VALID'
    ELSE 'INVALID'
  END AS order_status
FROM ranked r
WHERE r.rn = 1
"""
silver_customer_orders_df = spark.sql(silver_customer_orders_sql)
silver_customer_orders_df.createOrReplaceTempView("customer_orders_silver")

(
    silver_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ====================================================================================
# Target Table: silver.customer_orders_daily
# - Aggregate from silver.customer_orders (temp view: customer_orders_silver)
# - KPIs:
#   order_date
#   orders_count        = COUNT(DISTINCT CASE WHEN order_status='VALID' THEN order_id END)
#   total_order_amount  = SUM(CASE WHEN order_status='VALID' THEN order_amount ELSE 0 END)
#   avg_order_amount    = AVG(CASE WHEN order_status='VALID' THEN order_amount END)
# ====================================================================================
silver_customer_orders_daily_sql = """
SELECT
  sco.order_date AS order_date,
  COUNT(DISTINCT CASE WHEN sco.order_status = 'VALID' THEN sco.order_id END) AS orders_count,
  CAST(SUM(CASE WHEN sco.order_status = 'VALID' THEN sco.order_amount ELSE 0 END) AS DECIMAL(38,10)) AS total_order_amount,
  CAST(AVG(CASE WHEN sco.order_status = 'VALID' THEN sco.order_amount END) AS DECIMAL(38,10)) AS avg_order_amount
FROM customer_orders_silver sco
GROUP BY sco.order_date
"""
silver_customer_orders_daily_df = spark.sql(silver_customer_orders_daily_sql)

(
    silver_customer_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_daily.csv")
)

job.commit()
```