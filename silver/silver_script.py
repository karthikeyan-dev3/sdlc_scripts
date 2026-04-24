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
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
#    SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
bronze_customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

bronze_customer_order_items_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
bronze_customer_orders_df.createOrReplaceTempView("bronze_customer_orders")
bronze_customer_order_items_df.createOrReplaceTempView("bronze_customer_order_items")

# ===================================================================================
# TABLE: silver.customer_orders
# ===================================================================================

# 3) SQL transformation (clean + de-dup + rejects)
customer_orders_sql = """
WITH base AS (
  SELECT
    TRIM(co.order_id)                               AS order_id,
    CAST(co.order_time AS DATE)                     AS order_date,
    co.order_time                                   AS order_time,
    CAST(NULL AS STRING)                            AS customer_id,
    'COMPLETED'                                     AS order_status,
    CAST(co.order_total_amount AS DECIMAL(18,2))     AS order_total_amount,
    'USD'                                           AS currency_code
  FROM bronze_customer_orders co
  WHERE
    co.order_id IS NOT NULL
    AND co.order_time IS NOT NULL
    AND co.order_total_amount IS NOT NULL
    AND CAST(co.order_total_amount AS DECIMAL(18,2)) >= CAST(0.00 AS DECIMAL(18,2))
    AND TRIM(co.order_id) <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_time DESC
    ) AS rn
  FROM base
)
SELECT
  order_id,
  order_date,
  order_time,
  customer_id,
  order_status,
  order_total_amount,
  currency_code
FROM dedup
WHERE rn = 1
"""

silver_customer_orders_df = spark.sql(customer_orders_sql)
silver_customer_orders_df.createOrReplaceTempView("silver_customer_orders")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TABLE: silver.customer_order_items
# ===================================================================================

# 3) SQL transformation (clean + conformed to order headers + de-dup + rejects)
customer_order_items_sql = """
WITH base AS (
  SELECT
    TRIM(coi.order_id)                                                      AS order_id,
    sha2(concat(TRIM(coi.order_id), '|', TRIM(coi.product_id)), 256)         AS order_item_id,
    TRIM(coi.product_id)                                                    AS product_id,
    CAST(coi.quantity AS INT)                                               AS quantity,
    CASE
      WHEN CAST(coi.quantity AS INT) > 0
        THEN CAST(coi.line_amount AS DECIMAL(18,2)) / CAST(coi.quantity AS INT)
      ELSE CAST(NULL AS DECIMAL(18,4))
    END                                                                     AS unit_price,
    CAST(coi.line_amount AS DECIMAL(18,2))                                  AS line_total_amount
  FROM bronze_customer_order_items coi
  WHERE
    coi.order_id IS NOT NULL
    AND coi.product_id IS NOT NULL
    AND coi.quantity IS NOT NULL
    AND coi.line_amount IS NOT NULL
    AND TRIM(coi.order_id) <> ''
    AND TRIM(coi.product_id) <> ''
    AND CAST(coi.quantity AS INT) > 0
    AND CAST(coi.line_amount AS DECIMAL(18,2)) >= CAST(0.00 AS DECIMAL(18,2))
),
conformed AS (
  SELECT
    b.*
  FROM base b
  INNER JOIN silver_customer_orders sco
    ON b.order_id = sco.order_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id
      ORDER BY line_total_amount DESC
    ) AS rn
  FROM conformed
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  CAST(unit_price AS DECIMAL(18,4)) AS unit_price,
  line_total_amount
FROM dedup
WHERE rn = 1
"""

silver_customer_order_items_df = spark.sql(customer_order_items_sql)
silver_customer_order_items_df.createOrReplaceTempView("silver_customer_order_items")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_order_items.csv")
)

# ===================================================================================
# TABLE: silver.data_quality_daily
# ===================================================================================

# 3) SQL transformation (daily quality metrics)
data_quality_daily_sql = """
WITH counts AS (
  SELECT
    CAST(current_date() AS DATE) AS load_date,

    (
      (SELECT COUNT(*) FROM bronze_customer_orders)
      +
      (SELECT COUNT(*) FROM bronze_customer_order_items)
    ) AS source_record_count,

    (SELECT COUNT(*) FROM silver_customer_orders) AS loaded_order_count,
    (SELECT COUNT(*) FROM silver_customer_order_items) AS loaded_order_item_count
)
SELECT
  load_date,
  CAST(source_record_count AS BIGINT) AS source_record_count,
  CAST(loaded_order_count AS BIGINT) AS loaded_order_count,
  CAST(loaded_order_item_count AS BIGINT) AS loaded_order_item_count,
  CAST(
    source_record_count - (loaded_order_count + loaded_order_item_count)
    AS BIGINT
  ) AS rejected_record_count,
  CAST(
    CASE
      WHEN source_record_count > 0
        THEN round(((loaded_order_count + loaded_order_item_count) / source_record_count) * 100, 4)
      ELSE 100
    END
    AS DECIMAL(9,4)
  ) AS data_accuracy_pct
FROM counts
"""

silver_data_quality_daily_df = spark.sql(data_quality_daily_sql)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    silver_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/data_quality_daily.csv")
)

job.commit()
```