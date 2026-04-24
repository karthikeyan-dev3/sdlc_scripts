```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
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

# Ensure a single output file per target table (single CSV under TARGET_PATH)
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ===================================================================================
# SOURCE READS + TEMP VIEWS (Bronze)
# ===================================================================================

# bronze.customer_orders
df_customer_orders_bronze = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_customer_orders_bronze.createOrReplaceTempView("customer_orders")

# bronze.customer_order_items
df_customer_order_items_bronze = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_customer_order_items_bronze.createOrReplaceTempView("customer_order_items")

# ===================================================================================
# TARGET TABLE: silver.customer_orders
#  - De-duplicate to one record per order_id keeping latest order_timestamp
#  - order_date = CAST(order_timestamp AS DATE)
#  - customer_id = 'UNKNOWN'
#  - order_status = 'placed'
#  - currency_code = 'USD'
#  - order_total_amount carried forward
#  - source_file_date = CAST(order_timestamp AS DATE)
# ===================================================================================
sql_customer_orders = """
WITH dedup AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.order_id
      ORDER BY co.order_timestamp DESC
    ) AS rn
  FROM customer_orders co
)
SELECT
  CAST(TRIM(d.order_id) AS STRING)                                 AS order_id,
  CAST(d.order_timestamp AS DATE)                                  AS order_date,
  CAST('UNKNOWN' AS STRING)                                        AS customer_id,
  CAST('placed' AS STRING)                                         AS order_status,
  CAST('USD' AS STRING)                                            AS currency_code,
  CAST(d.order_total_amount AS DECIMAL(38, 10))                    AS order_total_amount,
  CAST(d.order_timestamp AS DATE)                                  AS source_file_date
FROM dedup d
WHERE d.rn = 1
"""

df_customer_orders_silver = spark.sql(sql_customer_orders)

# Write single CSV file directly under TARGET_PATH as: TARGET_PATH + "/customer_orders.csv"
(
    df_customer_orders_silver.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: silver.customer_order_items
#  - De-duplicate to one record per (order_id, product_id, transaction_time, store_id, quantity, line_sale_amount)
#  - order_item_id = HASH(order_id, product_id, store_id, transaction_time)
#  - enforce quantity > 0
#  - unit_price_amount = ROUND(line_sale_amount / NULLIF(quantity,0), 2)
#  - line_total_amount = line_sale_amount
# ===================================================================================
sql_customer_order_items = """
WITH filtered AS (
  SELECT
    coi.*
  FROM customer_order_items coi
  WHERE coi.quantity > 0
),
dedup AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        f.order_id,
        f.product_id,
        f.transaction_time,
        f.store_id,
        f.quantity,
        f.line_sale_amount
      ORDER BY f.transaction_time DESC
    ) AS rn
  FROM filtered f
)
SELECT
  CAST(TRIM(d.order_id) AS STRING)                                                                 AS order_id,
  CAST(
    sha2(
      concat_ws(
        '||',
        COALESCE(CAST(d.order_id AS STRING), ''),
        COALESCE(CAST(d.product_id AS STRING), ''),
        COALESCE(CAST(d.store_id AS STRING), ''),
        COALESCE(CAST(d.transaction_time AS STRING), '')
      ),
      256
    ) AS STRING
  )                                                                                                 AS order_item_id,
  CAST(TRIM(d.product_id) AS STRING)                                                                AS product_id,
  CAST(d.quantity AS INT)                                                                           AS quantity,
  CAST(ROUND(CAST(d.line_sale_amount AS DECIMAL(38, 10)) / NULLIF(CAST(d.quantity AS INT), 0), 2)
       AS DECIMAL(38, 2))                                                                           AS unit_price_amount,
  CAST(d.line_sale_amount AS DECIMAL(38, 10))                                                       AS line_total_amount
FROM dedup d
WHERE d.rn = 1
"""

df_customer_order_items_silver = spark.sql(sql_customer_order_items)

# Write single CSV file directly under TARGET_PATH as: TARGET_PATH + "/customer_order_items.csv"
(
    df_customer_order_items_silver.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: silver.daily_order_data_quality
#  - Grain: data_date = CAST(co.order_timestamp AS DATE)
#  - total_orders_count = COUNT(DISTINCT co.order_id)
#  - valid_orders_count = distinct orders meeting completeness & value rules and having at least one valid line
#  - invalid_orders_count = total - valid
#  - duplicate_orders_count = COUNT(*) - COUNT(DISTINCT co.order_id) within same data_date (based on bronze.customer_orders)
#  - dq_pass_rate = valid_orders_count / NULLIF(total_orders_count, 0)
# ===================================================================================
sql_daily_order_data_quality = """
SELECT
  CAST(co.order_timestamp AS DATE)                                                                 AS data_date,
  CAST(COUNT(DISTINCT co.order_id) AS INT)                                                          AS total_orders_count,
  CAST(
    COUNT(
      DISTINCT CASE
        WHEN co.order_id IS NOT NULL
         AND co.order_timestamp IS NOT NULL
         AND co.order_total_amount IS NOT NULL
         AND co.order_total_amount >= 0
         AND coi.quantity > 0
         AND coi.line_sale_amount IS NOT NULL
        THEN co.order_id
      END
    ) AS INT
  )                                                                                                 AS valid_orders_count,
  CAST(
    (
      COUNT(DISTINCT co.order_id)
      - COUNT(
          DISTINCT CASE
            WHEN co.order_id IS NOT NULL
             AND co.order_timestamp IS NOT NULL
             AND co.order_total_amount IS NOT NULL
             AND co.order_total_amount >= 0
             AND coi.quantity > 0
             AND coi.line_sale_amount IS NOT NULL
            THEN co.order_id
          END
        )
    ) AS INT
  )                                                                                                 AS invalid_orders_count,
  CAST((COUNT(*) - COUNT(DISTINCT co.order_id)) AS INT)                                              AS duplicate_orders_count,
  CAST(
    (
      COUNT(
        DISTINCT CASE
          WHEN co.order_id IS NOT NULL
           AND co.order_timestamp IS NOT NULL
           AND co.order_total_amount IS NOT NULL
           AND co.order_total_amount >= 0
           AND coi.quantity > 0
           AND coi.line_sale_amount IS NOT NULL
          THEN co.order_id
        END
      )
      / NULLIF(COUNT(DISTINCT co.order_id), 0)
    ) AS DECIMAL(38, 10)
  )                                                                                                 AS dq_pass_rate
FROM customer_orders co
LEFT JOIN customer_order_items coi
  ON co.order_id = coi.order_id
GROUP BY CAST(co.order_timestamp AS DATE)
"""

df_daily_order_data_quality_silver = spark.sql(sql_daily_order_data_quality)

# Write single CSV file directly under TARGET_PATH as: TARGET_PATH + "/daily_order_data_quality.csv"
(
    df_daily_order_data_quality_silver.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_order_data_quality.csv")
)

job.commit()
```