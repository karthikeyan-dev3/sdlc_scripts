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
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# CSV read options (adjust if your silver files differ)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE"
}

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    IMPORTANT: path format must be: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)

etl_data_quality_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/etl_data_quality_results_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")
etl_data_quality_results_silver_df.createOrReplaceTempView("etl_data_quality_results_silver")

# ======================================================================================
# TARGET TABLE: gold.gold_customer_orders (gco)
# Source mapping:
# silver.customer_orders_silver cos
# LEFT JOIN (SELECT order_id, COUNT(*) AS item_count FROM silver.customer_order_items_silver GROUP BY order_id) cois_cnt
#   ON cos.order_id = cois_cnt.order_id
# ======================================================================================
gold_customer_orders_sql = """
WITH cois_cnt AS (
  SELECT
    CAST(TRIM(order_id) AS STRING) AS order_id,
    CAST(COUNT(1) AS INT) AS item_count
  FROM customer_order_items_silver
  GROUP BY CAST(TRIM(order_id) AS STRING)
)
SELECT
  CAST(TRIM(cos.order_id) AS STRING)                                         AS order_id,
  CAST(cos.order_date AS DATE)                                               AS order_date,
  CAST(cos.customer_id AS STRING)                                            AS customer_id,
  CAST(TRIM(cos.order_status) AS STRING)                                     AS order_status,
  CAST(cos.order_total_amount AS DECIMAL(38, 10))                            AS order_total_amount,
  CAST(TRIM(cos.currency_code) AS STRING)                                    AS currency_code,
  CAST(cois_cnt.item_count AS INT)                                           AS item_count,
  CAST(TRIM(cos.source_system) AS STRING)                                    AS source_system,
  CAST(cos.load_date AS DATE)                                                AS load_date
FROM customer_orders_silver cos
LEFT JOIN cois_cnt
  ON CAST(TRIM(cos.order_id) AS STRING) = cois_cnt.order_id
"""

gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

# --------------------------------------------------------------------------------------
# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
#     Output path MUST be: TARGET_PATH + "/" + target_table.csv
# --------------------------------------------------------------------------------------
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ======================================================================================
# TARGET TABLE: gold.gold_customer_order_items (gcoi)
# Source mapping:
# silver.customer_order_items_silver cois
# ======================================================================================
gold_customer_order_items_sql = """
SELECT
  CAST(TRIM(cois.order_id) AS STRING)                                        AS order_id,
  CAST(TRIM(cois.order_item_id) AS STRING)                                   AS order_item_id,
  CAST(TRIM(cois.product_id) AS STRING)                                      AS product_id,
  CAST(cois.quantity AS INT)                                                 AS quantity,
  CAST(cois.unit_price AS DECIMAL(38, 10))                                   AS unit_price,
  CAST(cois.line_amount AS DECIMAL(38, 10))                                  AS line_amount,
  CAST(TRIM(cois.currency_code) AS STRING)                                   AS currency_code,
  CAST(cois.order_date AS DATE)                                              AS order_date,
  CAST(cois.customer_id AS STRING)                                           AS customer_id,
  CAST(cois.load_date AS DATE)                                               AS load_date
FROM customer_order_items_silver cois
"""

gold_customer_order_items_df = spark.sql(gold_customer_order_items_sql)

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ======================================================================================
# TARGET TABLE: gold.gold_etl_data_quality_results (gedqr)
# Source mapping:
# silver.etl_data_quality_results_silver edqrs
# Transformations:
# - total_records      = COUNT(edqrs.load_status)
# - valid_records      = SUM(CASE WHEN load_status='PASS' THEN 1 ELSE 0 END)
# - invalid_records    = SUM(CASE WHEN load_status='FAIL' THEN 1 ELSE 0 END)
# - accuracy_rate_pct  = (valid_records * 100.0) / total_records
# - load_status        = CASE WHEN invalid_records=0 THEN 'PASS' ELSE 'FAIL' END
# ======================================================================================
gold_etl_data_quality_results_sql = """
SELECT
  CAST(edqrs.process_date AS DATE) AS process_date,
  CAST(TRIM(edqrs.dataset_name) AS STRING) AS dataset_name,

  CAST(COUNT(edqrs.load_status) AS INT) AS total_records,

  CAST(SUM(CASE WHEN TRIM(edqrs.load_status) = 'PASS' THEN 1 ELSE 0 END) AS INT) AS valid_records,

  CAST(SUM(CASE WHEN TRIM(edqrs.load_status) = 'FAIL' THEN 1 ELSE 0 END) AS INT) AS invalid_records,

  CAST(
    (
      SUM(CASE WHEN TRIM(edqrs.load_status) = 'PASS' THEN 1 ELSE 0 END) * 100.0
    ) / COUNT(edqrs.load_status)
    AS DECIMAL(38, 10)
  ) AS accuracy_rate_pct,

  CAST(
    CASE
      WHEN SUM(CASE WHEN TRIM(edqrs.load_status) = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END
    AS STRING
  ) AS load_status

FROM etl_data_quality_results_silver edqrs
GROUP BY
  CAST(edqrs.process_date AS DATE),
  CAST(TRIM(edqrs.dataset_name) AS STRING)
"""

gold_etl_data_quality_results_df = spark.sql(gold_etl_data_quality_results_sql)

(
    gold_etl_data_quality_results_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_etl_data_quality_results.csv")
)

job.commit()
```