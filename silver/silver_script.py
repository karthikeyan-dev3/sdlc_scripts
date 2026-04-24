```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Job setup
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Config
# ----------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Consistent read options for CSV bronze
read_options = {
    "header": "true",
    "inferSchema": "true",
}

# ============================================================
# Read source tables (S3) + create temp views
# ============================================================
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

ingestion_metadata_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/ingestion_metadata_bronze.{FILE_FORMAT}/")
)
ingestion_metadata_bronze_df.createOrReplaceTempView("ingestion_metadata_bronze")

# ============================================================
# TARGET TABLE: customer_orders_silver
# ============================================================
customer_orders_silver_sql = """
WITH base AS (
  SELECT
      CAST(ob.order_id AS STRING)                                             AS order_id,
      CAST(DATE(ob.transaction_time) AS DATE)                                 AS order_date,
      CAST(NULL AS STRING)                                                    AS customer_id,
      CAST(CASE WHEN ob.sale_amount IS NOT NULL THEN 'COMPLETED' ELSE 'UNKNOWN' END AS STRING) AS order_status,
      CAST(ob.sale_amount AS DECIMAL(38, 10))                                 AS order_total_amount,
      CAST('USD' AS STRING)                                                   AS currency_code,
      CAST('sales_transactions_raw' AS STRING)                                AS source_system,
      CAST(COALESCE(DATE(imb.ingestion_timestamp), CURRENT_DATE) AS DATE)     AS ingestion_date,
      ob.transaction_time                                                     AS _txn_time,
      imb.ingestion_timestamp                                                 AS _ing_ts
  FROM orders_bronze ob
  LEFT JOIN ingestion_metadata_bronze imb
    ON imb.source_table_name = 'sales_transactions_raw'
   AND DATE(imb.ingestion_timestamp) = DATE(COALESCE(ob.transaction_time, imb.ingestion_timestamp))
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY COALESCE(_txn_time, _ing_ts) DESC
    ) AS rn
  FROM base
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
FROM dedup
WHERE rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ============================================================
# TARGET TABLE: customer_order_items_silver
# ============================================================
customer_order_items_silver_sql = """
WITH base AS (
  SELECT
      CAST(oib.order_id AS STRING) AS order_id,

      -- UDT asked for HASH(order_id, product_id). Use Spark SQL hash() and cast to string.
      CAST(hash(oib.order_id, oib.product_id) AS STRING) AS order_item_id,

      CAST(oib.product_id AS STRING) AS product_id,

      CAST(
        COALESCE(
          CASE WHEN oib.quantity < 0 THEN ABS(oib.quantity) ELSE oib.quantity END,
          0
        ) AS INT
      ) AS quantity,

      CAST(pb.price AS DECIMAL(38, 10)) AS unit_price_amount,

      CAST(
        COALESCE(
          CASE WHEN oib.quantity < 0 THEN ABS(oib.quantity) ELSE oib.quantity END,
          0
        ) * pb.price
        AS DECIMAL(38, 10)
      ) AS line_total_amount,

      CAST('USD' AS STRING) AS currency_code,
      CAST('sales_transactions_raw' AS STRING) AS source_system,
      CAST(COALESCE(DATE(imb.ingestion_timestamp), CURRENT_DATE) AS DATE) AS ingestion_date,

      imb.ingestion_timestamp AS _ing_ts
  FROM order_items_bronze oib
  LEFT JOIN products_bronze pb
    ON oib.product_id = pb.product_id
  LEFT JOIN ingestion_metadata_bronze imb
    ON imb.source_table_name = 'sales_transactions_raw'
   AND DATE(imb.ingestion_timestamp) = DATE(imb.ingestion_timestamp)
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, product_id
      ORDER BY _ing_ts DESC
    ) AS rn
  FROM base
)
SELECT
  order_id,
  order_item_id,
  product_id,
  quantity,
  unit_price_amount,
  line_total_amount,
  currency_code,
  source_system,
  ingestion_date
FROM dedup
WHERE rn = 1
"""

customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

# ============================================================
# TARGET TABLE: ingestion_metadata_silver
# ============================================================
ingestion_metadata_silver_sql = """
WITH base AS (
  SELECT
    CAST(imb.source_table_name AS STRING)        AS dataset_name,
    CAST(NULL AS STRING)                         AS source_location,
    CAST(imb.file_name AS STRING)                AS file_name,
    CAST(NULL AS STRING)                         AS file_format,
    CAST(DATE(imb.ingestion_timestamp) AS DATE)  AS ingestion_date,
    imb.ingestion_timestamp                      AS ingestion_timestamp
  FROM ingestion_metadata_bronze imb
),
agg AS (
  SELECT
    dataset_name,
    source_location,
    file_name,
    file_format,
    ingestion_date,
    MAX(ingestion_timestamp)                     AS load_timestamp,
    CAST(COUNT(*) AS INT)                        AS record_count,
    CAST(NULL AS INT)                            AS valid_record_count,
    CAST(NULL AS INT)                            AS invalid_record_count,
    CAST('UNKNOWN' AS STRING)                    AS validation_status
  FROM base
  GROUP BY
    dataset_name, source_location, file_name, file_format, ingestion_date
)
SELECT
  dataset_name,
  source_location,
  file_name,
  file_format,
  ingestion_date,
  load_timestamp,
  record_count,
  valid_record_count,
  invalid_record_count,
  validation_status
FROM agg
"""

ingestion_metadata_silver_df = spark.sql(ingestion_metadata_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    ingestion_metadata_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/ingestion_metadata_silver.csv")
)

job.commit()
```