```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------------------
# Glue / Spark setup
# --------------------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Helpful for consistent CSV parsing in Glue
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# --------------------------------------------------------------------------------------------------
# TABLE: silver.customer_orders
# 1) Read source
# 2) Create temp view(s)
# 3) Transform via Spark SQL (dedup with ROW_NUMBER)
# 4) Write single CSV file to TARGET_PATH/target_table.csv
# --------------------------------------------------------------------------------------------------
customer_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_src_df.createOrReplaceTempView("customer_orders")

customer_orders_df = spark.sql("""
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
  CAST(co.transaction_id AS STRING)                                     AS order_id,
  CAST(DATE(co.transaction_time) AS DATE)                               AS order_date,
  CAST(co.store_id AS STRING)                                           AS customer_id,
  CAST(co.sale_amount AS DECIMAL(38, 10))                               AS order_total_amount,
  CAST(co.transaction_time AS TIMESTAMP)                                AS created_at,
  CAST(co.transaction_time AS TIMESTAMP)                                AS updated_at,
  CAST(DATE(co.transaction_time) AS DATE)                               AS load_date,

  -- Defaults / conformed attributes (per table description)
  CAST('USD' AS STRING)                                                 AS currency_code,
  CAST('UNKNOWN' AS STRING)                                             AS payment_method,
  CAST('UNKNOWN' AS STRING)                                             AS shipping_method,
  CAST(0.0 AS DECIMAL(38, 10))                                          AS shipping_amount,
  CAST(0.0 AS DECIMAL(38, 10))                                          AS tax_amount,
  CAST(0.0 AS DECIMAL(38, 10))                                          AS discount_amount,
  CAST('COMPLETED' AS STRING)                                           AS order_status,

  -- Lineage
  CAST('bronze.customer_orders' AS STRING)                              AS source_system
FROM ranked co
WHERE co.rn = 1
""")

(
    customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# --------------------------------------------------------------------------------------------------
# TABLE: silver.customer_order_items
# 1) Read source
# 2) Create temp view(s)
# 3) Transform via Spark SQL (dedup with ROW_NUMBER)
# 4) Write single CSV file to TARGET_PATH/target_table.csv
# --------------------------------------------------------------------------------------------------
customer_order_items_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
customer_order_items_src_df.createOrReplaceTempView("customer_order_items")

customer_order_items_df = spark.sql("""
WITH ranked AS (
  SELECT
    coi.*,
    ROW_NUMBER() OVER (
      PARTITION BY coi.transaction_id, coi.product_id, coi.transaction_time
      ORDER BY coi.transaction_time DESC
    ) AS rn
  FROM customer_order_items coi
)
SELECT
  CAST(coi.transaction_id AS STRING)                                                   AS order_id,
  CAST(MD5(CONCAT(coi.transaction_id, coi.product_id, CAST(coi.transaction_time AS STRING))) AS STRING)
                                                                                       AS order_item_id,
  CAST(coi.product_id AS STRING)                                                       AS product_id,
  CAST(coi.quantity AS INT)                                                            AS quantity,
  CAST(
    CASE WHEN coi.quantity > 0 THEN (coi.sale_amount / coi.quantity) END
    AS DECIMAL(38, 10)
  )                                                                                    AS unit_price,
  CAST(coi.sale_amount AS DECIMAL(38, 10))                                             AS line_amount,
  CAST(DATE(coi.transaction_time) AS DATE)                                             AS load_date,

  -- Default / conformed attribute (per table description)
  CAST('USD' AS STRING)                                                                AS currency_code
FROM ranked coi
WHERE coi.rn = 1
""")

(
    customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items.csv")
)

# --------------------------------------------------------------------------------------------------
# TABLE: silver.data_quality_daily_orders
# 1) Read source
# 2) Create temp view(s)
# 3) Transform via Spark SQL (GROUP BY DATE(transaction_time))
# 4) Write single CSV file to TARGET_PATH/target_table.csv
# --------------------------------------------------------------------------------------------------
data_quality_daily_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_orders.{FILE_FORMAT}/")
)
data_quality_daily_orders_src_df.createOrReplaceTempView("data_quality_daily_orders")

data_quality_daily_orders_df = spark.sql("""
SELECT
  CAST(DATE(dqdo.transaction_time) AS DATE)                                           AS load_date,
  CAST('customer_orders' AS STRING)                                                   AS dataset_name,

  CAST(COUNT(DISTINCT dqdo.transaction_id) AS INT)                                    AS total_records,

  CAST(SUM(
    CASE
      WHEN dqdo.transaction_id IS NOT NULL
       AND dqdo.transaction_time IS NOT NULL
       AND dqdo.sale_amount IS NOT NULL
      THEN 1 ELSE 0
    END
  ) AS INT)                                                                           AS valid_records,

  CAST(SUM(
    CASE
      WHEN dqdo.transaction_id IS NULL
        OR dqdo.transaction_time IS NULL
        OR dqdo.sale_amount IS NULL
      THEN 1 ELSE 0
    END
  ) AS INT)                                                                           AS invalid_records,

  CAST(
    100.0 * SUM(
      CASE
        WHEN dqdo.transaction_id IS NOT NULL
         AND dqdo.transaction_time IS NOT NULL
         AND dqdo.sale_amount IS NOT NULL
        THEN 1 ELSE 0
      END
    ) / COUNT(1)
    AS DECIMAL(38, 10)
  )                                                                                   AS completeness_score_pct,

  CAST(
    100.0 * SUM(
      CASE
        WHEN dqdo.sale_amount IS NOT NULL AND dqdo.sale_amount >= 0
        THEN 1 ELSE 0
      END
    ) / COUNT(1)
    AS DECIMAL(38, 10)
  )                                                                                   AS accuracy_score_pct,

  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                                AS validation_run_timestamp
FROM data_quality_daily_orders dqdo
GROUP BY DATE(dqdo.transaction_time)
""")

(
    data_quality_daily_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_orders.csv")
)

job.commit()
```