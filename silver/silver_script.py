```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue setup
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
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze) and create temp views
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# -----------------------------------------------------------------------------------
# 2) Transform + Write: silver.customer_orders_silver
#    One row per order (de-duplicated on order_id using latest order_timestamp)
# -----------------------------------------------------------------------------------
customer_orders_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    cob.order_id                                                  AS order_id,
    CAST(cob.order_timestamp AS DATE)                             AS order_date,
    cob.store_id                                                  AS customer_id,
    'COMPLETED'                                                   AS order_status,
    cob.order_total_amount                                        AS order_total_amount,
    'USD'                                                         AS currency_code,
    'sales_transactions_raw'                                      AS source_system,
    CURRENT_DATE                                                  AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY cob.order_id
      ORDER BY cob.order_timestamp DESC
    )                                                             AS rn
  FROM customer_orders_bronze cob
)
SELECT
  order_id,
  order_date,
  customer_id,
  order_status,
  order_total_amount,
  currency_code,
  source_system,
  ingestion_date
FROM ranked
WHERE rn = 1
""")

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders_silver.csv")
)

customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# -----------------------------------------------------------------------------------
# 3) Transform + Write: silver.customer_order_items_silver
#    One row per order item (de-duplicated on (order_id, order_item_id) using latest transaction_timestamp)
# -----------------------------------------------------------------------------------
customer_order_items_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    coib.order_id                                                 AS order_id,
    coib.order_item_id                                            AS order_item_id,
    coib.product_id                                               AS product_id,
    coib.quantity                                                 AS quantity,
    CASE
      WHEN coib.quantity <> 0 THEN coib.line_amount / coib.quantity
      ELSE NULL
    END                                                           AS unit_price,
    coib.line_amount                                              AS line_amount,
    CURRENT_DATE                                                  AS ingestion_date,
    ROW_NUMBER() OVER (
      PARTITION BY coib.order_id, coib.order_item_id
      ORDER BY coib.transaction_timestamp DESC
    )                                                             AS rn
  FROM customer_order_items_bronze coib
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price,
  line_amount,
  ingestion_date
FROM ranked
WHERE rn = 1
""")

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

# -----------------------------------------------------------------------------------
# 4) Transform + Write: silver.customers_silver
#    Customer dimension (SCD2-ready) from orders; one current record per customer_id
# -----------------------------------------------------------------------------------
customers_silver_df = spark.sql("""
WITH base AS (
  SELECT
    cob.store_id                                                  AS customer_id,
    NULL                                                         AS customer_name,
    NULL                                                         AS customer_email,
    NULL                                                         AS customer_phone,
    NULL                                                         AS billing_country,
    NULL                                                         AS shipping_country,
    TRUE                                                         AS is_active,
    MIN(CAST(cob.order_timestamp AS DATE)) OVER (
      PARTITION BY cob.store_id
    )                                                            AS effective_start_date,
    DATE '9999-12-31'                                            AS effective_end_date
  FROM customer_orders_bronze cob
),
dedup AS (
  SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_phone,
    billing_country,
    shipping_country,
    is_active,
    effective_start_date,
    effective_end_date,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY effective_start_date ASC
    )                                                             AS rn
  FROM base
)
SELECT
  customer_id,
  customer_name,
  customer_email,
  customer_phone,
  billing_country,
  shipping_country,
  is_active,
  effective_start_date,
  effective_end_date
FROM dedup
WHERE rn = 1
""")

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customers_silver.csv")
)

customers_silver_df.createOrReplaceTempView("customers_silver")

# -----------------------------------------------------------------------------------
# 5) Transform + Write: silver.products_silver
#    Product dimension (SCD2-ready); de-duplicate on product_id (latest record if duplicates exist)
# -----------------------------------------------------------------------------------
products_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    pb.product_id                                                 AS product_id,
    pb.product_name                                               AS product_name,
    pb.category                                                   AS product_category,
    pb.is_active                                                  AS is_active,
    CURRENT_DATE                                                  AS effective_start_date,
    DATE '9999-12-31'                                            AS effective_end_date,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.updated_at DESC
    )                                                             AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  product_category,
  is_active,
  effective_start_date,
  effective_end_date
FROM ranked
WHERE rn = 1
""")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# -----------------------------------------------------------------------------------
# 6) Transform + Write: silver.daily_order_kpis_silver
#    Daily KPI pre-aggregation
# -----------------------------------------------------------------------------------
daily_order_kpis_silver_df = spark.sql("""
SELECT
  cos.order_date                                                  AS kpi_date,
  COUNT(DISTINCT cos.order_id)                                    AS orders_count,
  COALESCE(SUM(cois.quantity), 0)                                 AS items_count,
  COALESCE(SUM(cois.line_amount), 0)                              AS gross_sales_amount,
  CASE
    WHEN COUNT(DISTINCT cos.order_id) > 0
      THEN SUM(cos.order_total_amount) / COUNT(DISTINCT cos.order_id)
    ELSE 0
  END                                                            AS avg_order_value,
  COUNT(DISTINCT cos.customer_id)                                 AS distinct_customers_count,
  CURRENT_DATE                                                    AS ingestion_date
FROM customer_orders_silver cos
INNER JOIN customer_order_items_silver cois
  ON cos.order_id = cois.order_id
GROUP BY
  cos.order_date
""")

(
    daily_order_kpis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_order_kpis_silver.csv")
)

job.commit()
```