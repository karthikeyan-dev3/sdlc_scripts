```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Job setup (AWS Glue)
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# Tune Spark to produce single CSV outputs (still also coalesce(1) per table)
spark.conf.set("spark.sql.shuffle.partitions", "1")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 + create temp views
#    (STRICT SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/"))
# -----------------------------------------------------------------------------------
df_customer_orders = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_customer_orders.createOrReplaceTempView("customer_orders")

df_customer_order_line_items = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_items.{FILE_FORMAT}/")
)
df_customer_order_line_items.createOrReplaceTempView("customer_order_line_items")

df_etl_audit_lineage = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_audit_lineage.{FILE_FORMAT}/")
)
df_etl_audit_lineage.createOrReplaceTempView("etl_audit_lineage")

# ===================================================================================
# TARGET TABLE: silver.customer_orders
# -----------------------------------------------------------------------------------
# mapping_details:
#   bronze.customer_orders co
#   LEFT JOIN bronze.customer_order_line_items coli
#   ON co.transaction_id = coli.transaction_id
#
# Transformations (EXACT):
#   order_id            = co.transaction_id
#   order_date          = CAST(co.transaction_time AS DATE)
#   customer_id         = NULL
#   order_status        = 'COMPLETED'
#   currency_code       = 'USD'
#   order_total_amount  = co.sale_amount (validated >= 0)
#   item_count          = SUM(COALESCE(coli.quantity,0)) by transaction_id
# Dedupe:
#   keep latest co.transaction_time per co.transaction_id
# ===================================================================================
customer_orders_sql = """
WITH co_dedup AS (
  SELECT
    co.*,
    ROW_NUMBER() OVER (
      PARTITION BY co.transaction_id
      ORDER BY co.transaction_time DESC
    ) AS rn
  FROM customer_orders co
),
co_latest AS (
  SELECT
    *
  FROM co_dedup
  WHERE rn = 1
),
item_counts AS (
  SELECT
    co.transaction_id AS transaction_id,
    SUM(COALESCE(CAST(coli.quantity AS INT), 0)) AS item_count
  FROM customer_orders co
  LEFT JOIN customer_order_line_items coli
    ON co.transaction_id = coli.transaction_id
  GROUP BY co.transaction_id
)
SELECT
  CAST(co_latest.transaction_id AS STRING)                                     AS order_id,
  CAST(co_latest.transaction_time AS DATE)                                     AS order_date,
  CAST(NULL AS STRING)                                                        AS customer_id,
  CAST('COMPLETED' AS STRING)                                                  AS order_status,
  CAST('USD' AS STRING)                                                        AS currency_code,
  CAST(CASE WHEN co_latest.sale_amount >= 0 THEN co_latest.sale_amount END
       AS DECIMAL(38, 10))                                                     AS order_total_amount,
  CAST(COALESCE(item_counts.item_count, 0) AS INT)                             AS item_count
FROM co_latest
LEFT JOIN item_counts
  ON co_latest.transaction_id = item_counts.transaction_id
"""

df_customer_orders_silver = spark.sql(customer_orders_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (STRICT)
(
    df_customer_orders_silver.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: silver.customer_order_line_items
# -----------------------------------------------------------------------------------
# mapping_details:
#   bronze.customer_order_line_items coli
#
# Transformations (EXACT):
#   order_id           = coli.transaction_id
#   product_id         = coli.product_id
#   quantity           = coli.quantity (validated > 0)
#   line_item_id       = ROW_NUMBER() OVER (PARTITION BY coli.transaction_id ORDER BY coli.product_id)
#   unit_price_amount  = NULL
#   line_total_amount  = NULL
# Dedupe:
#   one row per (transaction_id, product_id) keeping max(quantity)
# ===================================================================================
customer_order_line_items_sql = """
WITH base AS (
  SELECT
    CAST(coli.transaction_id AS STRING) AS transaction_id,
    CAST(coli.product_id AS STRING)     AS product_id,
    CAST(coli.quantity AS INT)          AS quantity
  FROM customer_order_line_items coli
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    MAX(quantity) AS quantity
  FROM base
  GROUP BY transaction_id, product_id
),
final AS (
  SELECT
    transaction_id,
    product_id,
    quantity,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY product_id
    ) AS line_item_id
  FROM dedup
)
SELECT
  CAST(transaction_id AS STRING)                               AS order_id,
  CAST(product_id AS STRING)                                   AS product_id,
  CAST(CASE WHEN quantity > 0 THEN quantity END AS INT)         AS quantity,
  CAST(line_item_id AS BIGINT)                                  AS line_item_id,
  CAST(NULL AS DECIMAL(38, 10))                                 AS unit_price_amount,
  CAST(NULL AS DECIMAL(38, 10))                                 AS line_total_amount
FROM final
"""

df_customer_order_line_items_silver = spark.sql(customer_order_line_items_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (STRICT)
(
    df_customer_order_line_items_silver.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_line_items.csv")
)

# ===================================================================================
# TARGET TABLE: silver.etl_audit_lineage
# -----------------------------------------------------------------------------------
# mapping_details:
#   bronze.etl_audit_lineage eal
#
# Transformations (EXACT):
#   load_date         = CAST(eal.load_start_ts AS DATE)
#   source_system     = eal.source_schema
#   source_s3_path    = NULL
#   target_table_name = CASE WHEN eal.source_table='sales_transactions_raw'
#                            THEN 'silver.customer_orders|silver.customer_order_line_items'
#                            WHEN eal.source_table IN ('products_raw','stores_raw')
#                            THEN 'N/A'
#                            ELSE 'N/A' END
#   records_read      = eal.row_count
#   records_loaded    = CASE WHEN eal.status='SUCCESS' THEN eal.row_count ELSE 0 END
#   records_rejected  = 0
#   dq_error_count    = 0
#   etl_run_id        = eal.run_id
#   etl_start_ts      = eal.load_start_ts
#   etl_end_ts        = eal.load_end_ts
# Dedupe:
#   unique by (run_id, source_table, batch_id) keeping latest load_end_ts
# ===================================================================================
etl_audit_lineage_sql = """
WITH eal_dedup AS (
  SELECT
    eal.*,
    ROW_NUMBER() OVER (
      PARTITION BY eal.run_id, eal.source_table, eal.batch_id
      ORDER BY eal.load_end_ts DESC
    ) AS rn
  FROM etl_audit_lineage eal
)
SELECT
  CAST(eal.load_start_ts AS DATE) AS load_date,
  CAST(eal.source_schema AS STRING) AS source_system,
  CAST(NULL AS STRING) AS source_s3_path,
  CAST(
    CASE
      WHEN eal.source_table = 'sales_transactions_raw'
        THEN 'silver.customer_orders|silver.customer_order_line_items'
      WHEN eal.source_table IN ('products_raw', 'stores_raw')
        THEN 'N/A'
      ELSE 'N/A'
    END AS STRING
  ) AS target_table_name,
  CAST(eal.row_count AS BIGINT) AS records_read,
  CAST(
    CASE
      WHEN eal.status = 'SUCCESS' THEN eal.row_count
      ELSE 0
    END AS BIGINT
  ) AS records_loaded,
  CAST(0 AS BIGINT) AS records_rejected,
  CAST(0 AS BIGINT) AS dq_error_count,
  CAST(eal.run_id AS STRING) AS etl_run_id,
  CAST(eal.load_start_ts AS TIMESTAMP) AS etl_start_ts,
  CAST(eal.load_end_ts AS TIMESTAMP) AS etl_end_ts
FROM eal_dedup eal
WHERE eal.rn = 1
"""

df_etl_audit_lineage_silver = spark.sql(etl_audit_lineage_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (STRICT)
(
    df_etl_audit_lineage_silver.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_audit_lineage.csv")
)

job.commit()
```