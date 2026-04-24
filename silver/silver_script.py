```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source reads (Bronze) + Temp Views
# ------------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

# ====================================================================================
# TARGET TABLE: silver.customer_orders_silver
# - Dedup: latest record per transaction_id by transaction_time
# - Transformations:
#     cos.order_id = cob.transaction_id
#     cos.order_date = DATE(cob.transaction_time)
#     cus.customer_id = cob.store_id  (represented directly as customer_id on cos)
#     oss.order_status = 'COMPLETED'  (represented directly as order_status on cos)
#     cos.order_total_amount = cob.sale_amount
#     cur.currency_code = 'USD'       (represented directly as currency_code on cos)
# - Data quality rules (per description):
#     order_id not null
#     non-negative amount (sale_amount)
# ====================================================================================
customer_orders_silver_df = spark.sql("""
WITH dedup AS (
  SELECT
    cob.*,
    ROW_NUMBER() OVER (
      PARTITION BY cob.transaction_id
      ORDER BY cob.transaction_time DESC
    ) AS rn
  FROM customer_orders_bronze cob
)
SELECT
  CAST(TRIM(d.transaction_id) AS STRING)                         AS order_id,
  CAST(DATE(d.transaction_time) AS DATE)                         AS order_date,
  CAST(TRIM(d.store_id) AS STRING)                               AS customer_id,
  CAST('COMPLETED' AS STRING)                                    AS order_status,
  CAST(d.sale_amount AS DECIMAL(38, 10))                         AS order_total_amount,
  CAST('USD' AS STRING)                                          AS currency_code
FROM dedup d
WHERE d.rn = 1
  AND d.transaction_id IS NOT NULL
  AND COALESCE(CAST(d.sale_amount AS DECIMAL(38, 10)), CAST(0 AS DECIMAL(38, 10))) >= CAST(0 AS DECIMAL(38, 10))
""")

(
    customer_orders_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ====================================================================================
# TARGET TABLE: silver.order_statuses_silver
# - Derived reference (single row) as per UDT:
#     oss.order_status = 'COMPLETED'
# ====================================================================================
order_statuses_silver_df = spark.sql("""
SELECT
  CAST('COMPLETED' AS STRING) AS order_status
GROUP BY 1
""")

(
    order_statuses_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_statuses_silver.csv")
)

# ====================================================================================
# TARGET TABLE: silver.currencies_silver
# - Derived reference (single row) as per UDT:
#     cur.currency_code = 'USD'
# ====================================================================================
currencies_silver_df = spark.sql("""
SELECT
  CAST('USD' AS STRING) AS currency_code
GROUP BY 1
""")

(
    currencies_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/currencies_silver.csv")
)

# ====================================================================================
# TARGET TABLE: silver.customers_silver
# - Placeholder customer reference as per UDT column mapping:
#     cus.customer_id = cob.store_id
# - Build distinct customer_id values from bronze.store_id
# ====================================================================================
customers_silver_df = spark.sql("""
SELECT
  CAST(TRIM(cob.store_id) AS STRING) AS customer_id
FROM customer_orders_bronze cob
WHERE cob.store_id IS NOT NULL
GROUP BY 1
""")

(
    customers_silver_df
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customers_silver.csv")
)

job.commit()
```