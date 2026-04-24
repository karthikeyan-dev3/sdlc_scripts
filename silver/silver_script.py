```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Parameters / Constants
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Recommended for consistent single-file writes
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------------------------------------------------------------
# 1) Read SOURCE tables from S3 (Bronze)
# -----------------------------------------------------------------------------------
bronze_customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
bronze_customer_orders_df.createOrReplaceTempView("bronze_customer_orders")

bronze_customer_order_items_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
bronze_customer_order_items_df.createOrReplaceTempView("bronze_customer_order_items")

# ===================================================================================
# TARGET TABLE: silver.customer_orders
# - Deduplicate to one record per order_id (latest order_time).
# - Conform columns per UDT.
# - order_id not null
# - order_date = CAST(order_time AS DATE)
# - order_status = 'UNKNOWN'
# - customer_id = NULL
# - currency_code = 'UNKNOWN'
# - ingestion_date = CURRENT_DATE
# ===================================================================================
customer_orders_sql = """
WITH ranked AS (
  SELECT
    co.order_id                                            AS order_id,
    CAST(co.order_time AS DATE)                            AS order_date,
    CAST(NULL AS STRING)                                   AS customer_id,
    CAST('UNKNOWN' AS STRING)                              AS order_status,
    co.order_total_amount                                  AS order_total_amount,
    CAST('UNKNOWN' AS STRING)                              AS currency_code,
    CURRENT_DATE                                           AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY co.order_id
      ORDER BY co.order_time DESC NULLS LAST
    ) AS rn
  FROM bronze_customer_orders co
  WHERE co.order_id IS NOT NULL
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_total_amount,
  currency_code,
  ingestion_date
FROM ranked
WHERE rn = 1
"""
customer_orders_df = spark.sql(customer_orders_sql)
customer_orders_df.createOrReplaceTempView("silver_customer_orders")

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: silver.customer_order_items
# - Join to existing orders (silver.customer_orders) to enforce linkage
# - Deduplicate item rows:
#   QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id, quantity, line_amount
#                             ORDER BY sco.order_date DESC) = 1
# - Derive order_item_id:
#   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY product_id, COALESCE(quantity,0), COALESCE(line_amount,0))
# - Conform:
#   quantity = COALESCE(quantity,0)
#   unit_price = CASE WHEN quantity > 0 THEN line_amount / quantity ELSE NULL END
#   line_total_amount = line_amount
# ===================================================================================
customer_order_items_sql = """
WITH joined AS (
  SELECT
    coi.order_id                 AS order_id,
    coi.product_id               AS product_id,
    coi.quantity                 AS quantity_raw,
    coi.line_amount              AS line_amount,
    sco.order_date               AS order_date
  FROM bronze_customer_order_items coi
  INNER JOIN silver_customer_orders sco
    ON coi.order_id = sco.order_id
),
dedup AS (
  SELECT
    order_id,
    product_id,
    quantity_raw,
    line_amount,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id, quantity_raw, line_amount
      ORDER BY order_date DESC
    ) AS rn_dedup
  FROM joined
),
final AS (
  SELECT
    order_id,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY product_id, COALESCE(quantity_raw, 0), COALESCE(line_amount, 0)
    )                                                     AS order_item_id,
    product_id                                            AS product_id,
    COALESCE(quantity_raw, 0)                             AS quantity,
    CASE
      WHEN COALESCE(quantity_raw, 0) > 0 THEN (line_amount / COALESCE(quantity_raw, 0))
      ELSE NULL
    END                                                   AS unit_price,
    line_amount                                           AS line_total_amount
  FROM dedup
  WHERE rn_dedup = 1
)
SELECT
  order_id,
  CAST(order_item_id AS INT) AS order_item_id,
  product_id,
  CAST(quantity AS INT)      AS quantity,
  unit_price,
  line_total_amount
FROM final
"""
customer_order_items_df = spark.sql(customer_order_items_sql)
customer_order_items_df.createOrReplaceTempView("silver_customer_order_items")

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: silver.orders_daily_kpis
# - KPI aggregation at kpi_date = sco.order_date
# - total_orders = COUNT(DISTINCT sco.order_id)
# - total_order_amount = SUM(DISTINCT sco.order_total_amount)
# - validated_orders = COUNT(DISTINCT CASE WHEN sco.order_id IS NOT NULL THEN sco.order_id END)
# - rejected_orders = 0
# ===================================================================================
orders_daily_kpis_sql = """
SELECT
  sco.order_date                                                          AS kpi_date,
  COUNT(DISTINCT sco.order_id)                                             AS total_orders,
  SUM(DISTINCT sco.order_total_amount)                                     AS total_order_amount,
  COUNT(DISTINCT CASE WHEN sco.order_id IS NOT NULL THEN sco.order_id END) AS validated_orders,
  CAST(0 AS INT)                                                           AS rejected_orders
FROM silver_customer_orders sco
LEFT JOIN silver_customer_order_items scoi
  ON sco.order_id = scoi.order_id
GROUP BY sco.order_date
"""
orders_daily_kpis_df = spark.sql(orders_daily_kpis_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    orders_daily_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_daily_kpis.csv")
)

job.commit()
```