```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Bootstrap
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

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# -----------------------------------------------------------------------------------
# 1) Read Source Tables (Bronze) + Temp Views
#    (Paths MUST follow: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
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

etl_data_quality_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_results_bronze.{FILE_FORMAT}/")
)
etl_data_quality_results_bronze_df.createOrReplaceTempView("etl_data_quality_results_bronze")

# ===================================================================================
# TARGET TABLE: silver.customer_orders_silver
# - De-duplicate by transaction_id keeping latest transaction_time
# - order_id = transaction_id
# - order_date = CAST(transaction_time AS DATE)
# - customer_id = store_id
# - order_status = CASE WHEN sale_amount IS NOT NULL THEN 'COMPLETED' ELSE 'UNKNOWN' END
# - order_total_amount = COALESCE(sale_amount,0)
# - currency_code = 'USD'
# - item_count = NULL
# - source_system = 'sales_transactions_raw'
# - load_date = CURRENT_DATE
# ===================================================================================
customer_orders_silver_sql = """
WITH dedup AS (
  SELECT
    cob.*,
    ROW_NUMBER() OVER (
      PARTITION BY cob.transaction_id
      ORDER BY cob.transaction_time DESC
    ) AS rn
  FROM customer_orders_bronze cob
)
SELECT
  CAST(dedup.transaction_id AS STRING)                                     AS order_id,
  CAST(dedup.transaction_time AS DATE)                                     AS order_date,
  CAST(dedup.store_id AS STRING)                                           AS customer_id,
  CASE
    WHEN dedup.sale_amount IS NOT NULL THEN 'COMPLETED'
    ELSE 'UNKNOWN'
  END                                                                      AS order_status,
  CAST(COALESCE(dedup.sale_amount, 0) AS DECIMAL(38, 10))                  AS order_total_amount,
  'USD'                                                                    AS currency_code,
  CAST(NULL AS INT)                                                        AS item_count,
  'sales_transactions_raw'                                                 AS source_system,
  CURRENT_DATE                                                             AS load_date
FROM dedup
WHERE rn = 1
"""
customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

customer_orders_silver_output_path = f"{TARGET_PATH}/customer_orders_silver.csv"
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(customer_orders_silver_output_path)
)

# ===================================================================================
# TARGET TABLE: silver.customer_order_items_silver
# - Join: coib INNER JOIN cob ON transaction_id
# - De-duplicate by (transaction_id, product_id) keeping max quantity
# - order_id = coib.transaction_id
# - order_item_id = MD5(CONCAT(transaction_id,'|',COALESCE(product_id,'UNKNOWN')))
# - product_id = COALESCE(product_id,'UNKNOWN')
# - quantity = COALESCE(quantity,0)
# - unit_price = CASE WHEN quantity>0 THEN cob.sale_amount/quantity ELSE NULL END
# - line_amount = CASE WHEN quantity>0 THEN (cob.sale_amount/quantity)*quantity ELSE 0 END
# - currency_code = 'USD'
# - order_date = CAST(cob.transaction_time AS DATE)
# - customer_id = cob.store_id
# - load_date = CURRENT_DATE
# - source_system = 'sales_transactions_raw'
# ===================================================================================
customer_order_items_silver_sql = """
WITH joined AS (
  SELECT
    coib.transaction_id,
    coib.product_id,
    coib.quantity,
    cob.sale_amount,
    cob.transaction_time,
    cob.store_id
  FROM customer_order_items_bronze coib
  INNER JOIN customer_orders_bronze cob
    ON coib.transaction_id = cob.transaction_id
),
dedup AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY joined.transaction_id, COALESCE(joined.product_id, 'UNKNOWN')
      ORDER BY COALESCE(joined.quantity, 0) DESC
    ) AS rn
  FROM joined
)
SELECT
  CAST(dedup.transaction_id AS STRING)                                                     AS order_id,
  MD5(CONCAT(CAST(dedup.transaction_id AS STRING), '|', COALESCE(CAST(dedup.product_id AS STRING), 'UNKNOWN')))
                                                                                           AS order_item_id,
  COALESCE(CAST(dedup.product_id AS STRING), 'UNKNOWN')                                     AS product_id,
  CAST(COALESCE(dedup.quantity, 0) AS INT)                                                  AS quantity,
  CAST(
    CASE
      WHEN COALESCE(dedup.quantity, 0) > 0 THEN dedup.sale_amount / COALESCE(dedup.quantity, 0)
      ELSE NULL
    END
    AS DECIMAL(38, 10)
  )                                                                                         AS unit_price,
  CAST(
    CASE
      WHEN COALESCE(dedup.quantity, 0) > 0 THEN (dedup.sale_amount / COALESCE(dedup.quantity, 0)) * COALESCE(dedup.quantity, 0)
      ELSE 0
    END
    AS DECIMAL(38, 10)
  )                                                                                         AS line_amount,
  'USD'                                                                                     AS currency_code,
  CAST(dedup.transaction_time AS DATE)                                                      AS order_date,
  CAST(dedup.store_id AS STRING)                                                            AS customer_id,
  CURRENT_DATE                                                                              AS load_date,
  'sales_transactions_raw'                                                                  AS source_system
FROM dedup
WHERE rn = 1
"""
customer_order_items_silver_df = spark.sql(customer_order_items_silver_sql)

customer_order_items_silver_output_path = f"{TARGET_PATH}/customer_order_items_silver.csv"
(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(customer_order_items_silver_output_path)
)

# ===================================================================================
# TARGET TABLE: silver.etl_data_quality_results_silver
# - De-duplicate by (source_transaction_id, dq_rule_name) keeping latest source_transaction_time
# - process_date = CAST(source_transaction_time AS DATE)
# - dataset_name = 'customer_orders'
# - load_status = CASE WHEN dq_pass_flag THEN 'PASS' ELSE 'FAIL' END
# - (Also include dq_rule_name, source_transaction_id, dq_pass_flag, source_transaction_time)
# - source_system = 'sales_transactions_raw'
# - load_date = CURRENT_DATE
# ===================================================================================
etl_data_quality_results_silver_sql = """
WITH staged AS (
  SELECT
    edqrb.*,
    ROW_NUMBER() OVER (
      PARTITION BY edqrb.source_transaction_id, edqrb.dq_rule_name
      ORDER BY edqrb.source_transaction_time DESC
    ) AS rn
  FROM etl_data_quality_results_bronze edqrb
)
SELECT
  CAST(staged.source_transaction_id AS STRING)                                 AS source_transaction_id,
  TRIM(CAST(staged.dq_rule_name AS STRING))                                    AS dq_rule_name,
  CAST(staged.dq_pass_flag AS BOOLEAN)                                         AS dq_pass_flag,
  staged.source_transaction_time                                               AS source_transaction_time,
  CAST(staged.source_transaction_time AS DATE)                                 AS process_date,
  'customer_orders'                                                            AS dataset_name,
  CASE
    WHEN CAST(staged.dq_pass_flag AS BOOLEAN) THEN 'PASS'
    ELSE 'FAIL'
  END                                                                          AS load_status,
  'sales_transactions_raw'                                                     AS source_system,
  CURRENT_DATE                                                                 AS load_date
FROM staged
WHERE rn = 1
"""
etl_data_quality_results_silver_df = spark.sql(etl_data_quality_results_silver_sql)

etl_data_quality_results_silver_output_path = f"{TARGET_PATH}/etl_data_quality_results_silver.csv"
(
    etl_data_quality_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(etl_data_quality_results_silver_output_path)
)

job.commit()
```