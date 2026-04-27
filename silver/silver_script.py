```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Ensure Spark SQL behaves predictably for timestamps/dates (optional but recommended)
spark.sql("SET spark.sql.session.timeZone=UTC")

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
#    NOTE: Path must be constructed as: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views (Bronze)
# --------------------------------------------------------------------------------------
orders_bronze_df.createOrReplaceTempView("bronze_orders_bronze")
order_items_bronze_df.createOrReplaceTempView("bronze_order_items_bronze")

# ======================================================================================
# TABLE: silver.orders_silver
# ======================================================================================
# 3) Transformation SQL (apply exact UDT logic; Spark equivalent for QUALIFY)
orders_silver_sql = """
WITH ranked AS (
  SELECT
    transaction_id AS order_id,
    transaction_time AS order_ts,
    CAST(transaction_time AS DATE) AS order_date,
    store_id AS customer_id,
    sale_amount AS order_amount,
    'USD' AS currency_code,
    'COMPLETED' AS order_status,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
  FROM bronze_orders_bronze
)
SELECT
  order_id,
  order_ts,
  order_date,
  customer_id,
  order_amount,
  currency_code,
  order_status
FROM ranked
WHERE rn = 1
"""

orders_silver_df = spark.sql(orders_silver_sql)

# Create temp views (Silver)
orders_silver_df.createOrReplaceTempView("silver_orders_silver")

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ======================================================================================
# TABLE: silver.order_items_silver
# ======================================================================================
# 3) Transformation SQL (apply exact UDT logic; Spark equivalents for MD5/concat and QUALIFY)
order_items_silver_sql = """
WITH ranked AS (
  SELECT
    transaction_id AS order_id,
    md5(
      concat(
        coalesce(transaction_id, ''), '|',
        coalesce(product_id, ''), '|',
        coalesce(CAST(quantity AS STRING), ''), '|',
        coalesce(CAST(sale_amount AS STRING), '')
      )
    ) AS order_item_id,
    product_id,
    COALESCE(quantity, 0) AS quantity,
    CASE
      WHEN COALESCE(quantity, 0) > 0 THEN sale_amount / quantity
      ELSE NULL
    END AS unit_price,
    sale_amount AS line_amount,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, product_id ORDER BY transaction_id) AS rn
  FROM bronze_order_items_bronze
  WHERE transaction_id IS NOT NULL
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount
FROM ranked
WHERE rn = 1
"""

order_items_silver_df = spark.sql(order_items_silver_sql)

# Create temp views (Silver)
order_items_silver_df.createOrReplaceTempView("silver_order_items_silver")

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ======================================================================================
# TABLE: silver.customer_orders_daily_silver
# ======================================================================================
# 3) Transformation SQL (apply exact UDT logic)
customer_orders_daily_silver_sql = """
SELECT
  os.order_date,
  os.customer_id,
  COUNT(DISTINCT os.order_id) AS total_orders,
  COALESCE(SUM(ois.quantity), 0) AS total_items,
  SUM(os.order_amount) AS total_order_amount,
  CASE
    WHEN COUNT(DISTINCT os.order_id) > 0 THEN SUM(os.order_amount) / COUNT(DISTINCT os.order_id)
    ELSE NULL
  END AS avg_order_amount,
  MIN(os.order_ts) AS first_order_ts,
  MAX(os.order_ts) AS last_order_ts
FROM silver_orders_silver os
LEFT JOIN silver_order_items_silver ois
  ON os.order_id = ois.order_id
GROUP BY
  os.order_date,
  os.customer_id
"""

customer_orders_daily_silver_df = spark.sql(customer_orders_daily_silver_sql)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
(
    customer_orders_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders_daily_silver.csv")
)

# ======================================================================================
# TABLE: silver.etl_data_quality_daily_silver
# ======================================================================================
# 3) Transformation SQL (apply exact UDT logic; Spark equivalents for ::DECIMAL casts)
etl_data_quality_daily_silver_sql = """
SELECT
  run_date,
  source_system,
  dataset_name,
  records_ingested,
  records_valid,
  records_rejected,
  CASE
    WHEN records_ingested > 0
      THEN (CAST(records_valid AS DECIMAL(18,4)) / CAST(records_ingested AS DECIMAL(18,4))) * 100
    ELSE 0
  END AS data_quality_score_pct,
  sla_on_time_flag
FROM (
  SELECT
    CURRENT_DATE() AS run_date,
    'sales_transactions_raw' AS source_system,
    'orders' AS dataset_name,
    COUNT(*) AS records_ingested,
    SUM(CASE WHEN transaction_id IS NOT NULL AND transaction_time IS NOT NULL THEN 1 ELSE 0 END) AS records_valid,
    SUM(CASE WHEN transaction_id IS NULL OR transaction_time IS NULL THEN 1 ELSE 0 END) AS records_rejected,
    1 AS sla_on_time_flag
  FROM bronze_orders_bronze

  UNION ALL

  SELECT
    CURRENT_DATE() AS run_date,
    'sales_transactions_raw' AS source_system,
    'order_items' AS dataset_name,
    COUNT(*) AS records_ingested,
    SUM(CASE WHEN transaction_id IS NOT NULL AND product_id IS NOT NULL AND quantity IS NOT NULL THEN 1 ELSE 0 END) AS records_valid,
    SUM(CASE WHEN transaction_id IS NULL OR product_id IS NULL OR quantity IS NULL THEN 1 ELSE 0 END) AS records_rejected,
    1 AS sla_on_time_flag
  FROM bronze_order_items_bronze
) x
"""

etl_data_quality_daily_silver_df = spark.sql(etl_data_quality_daily_silver_sql)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
(
    etl_data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/etl_data_quality_daily_silver.csv")
)

job.commit()
```