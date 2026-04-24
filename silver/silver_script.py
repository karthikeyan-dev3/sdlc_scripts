```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended for consistent SQL behavior
spark.sql("SET spark.sql.legacy.timeParserPolicy=LEGACY")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 + create temp views (bronze)
# ------------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

order_lines_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_lines_bronze.{FILE_FORMAT}/")
)
order_lines_bronze_df.createOrReplaceTempView("order_lines_bronze")

# ====================================================================================
# TABLE: silver.orders_silver
# - De-duplicate to one record per order_id (latest order_time)
# - Conform types + derive order_date
# - Add load_timestamp
# ====================================================================================
orders_silver_df = spark.sql("""
WITH dedup AS (
  SELECT
    ob.*,
    ROW_NUMBER() OVER (PARTITION BY ob.order_id ORDER BY ob.order_time DESC) AS rn
  FROM orders_bronze ob
)
SELECT
  CAST(ob.order_id AS STRING)                                 AS order_id,
  CAST(ob.store_id AS STRING)                                 AS store_id,
  CAST(ob.order_time AS TIMESTAMP)                            AS order_time,
  CAST(CAST(ob.order_time AS TIMESTAMP) AS DATE)              AS order_date,
  CAST(ob.order_total_amount AS DECIMAL(18,2))                AS order_total_amount,
  current_timestamp()                                         AS load_timestamp
FROM dedup ob
WHERE ob.rn = 1
""")
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ====================================================================================
# TABLE: silver.order_lines_silver
# - De-duplicate to one record per (order_id, product_id)
# - Conform types
# - Enforce non-negative quantity/amounts via CASE -> 0
# - Add load_timestamp
# ====================================================================================
order_lines_silver_df = spark.sql("""
WITH dedup AS (
  SELECT
    olb.*,
    ROW_NUMBER() OVER (
      PARTITION BY olb.order_id, olb.product_id
      ORDER BY olb.order_id
    ) AS rn
  FROM order_lines_bronze olb
)
SELECT
  CAST(olb.order_id AS STRING)                                AS order_id,
  CAST(olb.product_id AS STRING)                              AS product_id,
  CAST(CASE WHEN olb.quantity > 0 THEN olb.quantity ELSE 0 END AS INT)              AS quantity,
  CAST(CASE WHEN olb.line_amount >= 0 THEN olb.line_amount ELSE 0 END AS DECIMAL(18,2)) AS line_amount,
  current_timestamp()                                         AS load_timestamp
FROM dedup olb
WHERE olb.rn = 1
""")
order_lines_silver_df.createOrReplaceTempView("order_lines_silver")

(
    order_lines_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_lines_silver.csv")
)

# ====================================================================================
# TABLE: silver.order_validation_silver
# - Order-level validation outcomes
# - valid_order rules:
#   order_id not null, order_time not null, order_total_amount >= 0,
#   invalid_line_cnt = 0, valid_line_cnt > 0
# - invalid_reason_code per UDT
# - Add load_timestamp
# ====================================================================================
order_validation_silver_df = spark.sql("""
WITH lc AS (
  SELECT
    order_id,
    SUM(CASE WHEN quantity > 0 AND line_amount >= 0 THEN 1 ELSE 0 END) AS valid_line_cnt,
    SUM(CASE WHEN quantity <= 0 OR line_amount < 0 THEN 1 ELSE 0 END)  AS invalid_line_cnt
  FROM order_lines_silver
  GROUP BY order_id
)
SELECT
  CAST(os.order_id AS STRING)                                                    AS order_id,
  CAST(COALESCE(lc.valid_line_cnt, 0) AS INT)                                     AS valid_line_cnt,
  CAST(COALESCE(lc.invalid_line_cnt, 0) AS INT)                                   AS invalid_line_cnt,
  CASE
    WHEN os.order_id IS NOT NULL
     AND os.order_time IS NOT NULL
     AND os.order_total_amount >= 0
     AND COALESCE(lc.invalid_line_cnt, 0) = 0
     AND COALESCE(lc.valid_line_cnt, 0) > 0
    THEN TRUE ELSE FALSE
  END                                                                            AS is_valid_order,
  CAST(
    CASE
      WHEN os.order_id IS NULL THEN 'MISSING_ORDER_ID'
      WHEN os.order_time IS NULL THEN 'MISSING_ORDER_TIME'
      WHEN os.order_total_amount < 0 THEN 'NEGATIVE_ORDER_TOTAL'
      WHEN COALESCE(lc.valid_line_cnt, 0) = 0 THEN 'NO_VALID_LINES'
      WHEN COALESCE(lc.invalid_line_cnt, 0) > 0 THEN 'HAS_INVALID_LINES'
      ELSE 'VALID'
    END
    AS STRING
  )                                                                              AS invalid_reason_code,
  current_timestamp()                                                            AS load_timestamp
FROM orders_silver os
LEFT JOIN lc
  ON os.order_id = lc.order_id
""")
order_validation_silver_df.createOrReplaceTempView("order_validation_silver")

(
    order_validation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_validation_silver.csv")
)

# ====================================================================================
# TABLE: silver.order_amounts_silver
# - Monetary measures for gold
# - gross_sales_amount = CASE WHEN is_valid_order THEN order_total_amount ELSE 0 END
# - net_sales_amount = gross_sales_amount
# - discount/tax/shipping = 0
# - order_date = CAST(order_time AS DATE)
# - Add load_timestamp
# ====================================================================================
order_amounts_silver_df = spark.sql("""
SELECT
  CAST(os.order_id AS STRING)                                                     AS order_id,
  CAST(CAST(os.order_time AS TIMESTAMP) AS DATE)                                   AS order_date,
  CAST(
    CASE WHEN ovs.is_valid_order THEN os.order_total_amount ELSE 0 END
    AS DECIMAL(18,2)
  )                                                                               AS gross_sales_amount,
  CAST(
    CASE WHEN ovs.is_valid_order THEN os.order_total_amount ELSE 0 END
    AS DECIMAL(18,2)
  )                                                                               AS net_sales_amount,
  CAST(0 AS DECIMAL(18,2))                                                        AS discount_amount,
  CAST(0 AS DECIMAL(18,2))                                                        AS tax_amount,
  CAST(0 AS DECIMAL(18,2))                                                        AS shipping_amount,
  current_timestamp()                                                             AS load_timestamp
FROM orders_silver os
LEFT JOIN order_validation_silver ovs
  ON os.order_id = ovs.order_id
""")
order_amounts_silver_df.createOrReplaceTempView("order_amounts_silver")

(
    order_amounts_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_amounts_silver.csv")
)

job.commit()
```