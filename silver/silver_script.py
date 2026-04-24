```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------------------------------------------------------------
# AWS Glue Setup
# ----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended for Spark SQL legacy hash behavior consistency (optional)
# spark.conf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")

# ----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
#    SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

order_data_quality_summary_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_data_quality_summary.{FILE_FORMAT}/")
)

# ----------------------------------------------------------------------------------
# 2) Create temp views
# ----------------------------------------------------------------------------------
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")                 # co
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")       # coi
order_data_quality_summary_bronze_df.createOrReplaceTempView("order_data_quality_summary_bronze")  # odqs

# ==================================================================================
# TARGET TABLE: silver.customer_orders
# ==================================================================================

# Deduplicate bronze.customer_orders on transaction_id (keep latest by transaction_time)
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_orders_dedup AS
SELECT
  co.*,
  ROW_NUMBER() OVER (
    PARTITION BY co.transaction_id
    ORDER BY co.transaction_time DESC
  ) AS rn
FROM customer_orders_bronze co
""")

# Aggregate items to compute order_total_amount and item_count at transaction_id grain
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_order_items_agg AS
SELECT
  coi.transaction_id,
  CAST(COALESCE(SUM(CAST(coi.sale_amount AS DECIMAL(18,2))), 0) AS DECIMAL(18,2)) AS order_total_amount,
  CAST(COALESCE(COUNT(*), 0) AS INT) AS item_count
FROM customer_order_items_bronze coi
GROUP BY coi.transaction_id
""")

customer_orders_silver_df = spark.sql("""
SELECT
  -- Identifiers / conformed columns
  CAST(TRIM(co.transaction_id) AS STRING)                                    AS order_id,
  CAST(CAST(co.transaction_time AS DATE) AS DATE)                            AS order_date,
  CAST(TRIM(co.store_id) AS STRING)                                          AS customer_id,

  -- Domain defaults per UDT
  CAST('UNKNOWN' AS STRING)                                                  AS order_status,
  CAST('USD' AS STRING)                                                      AS currency_code,

  -- Amount standardization / aggregation from items
  CAST(COALESCE(coia.order_total_amount, 0) AS DECIMAL(18,2))                AS order_total_amount,
  CAST(COALESCE(coia.item_count, 0) AS INT)                                  AS item_count,

  -- Lineage fields
  CAST('sales_transactions_raw' AS STRING)                                   AS source_system,
  CAST(CAST(co.transaction_time AS DATE) AS DATE)                            AS ingestion_date,
  CAST(co.transaction_time AS TIMESTAMP)                                     AS load_timestamp
FROM customer_orders_dedup co
LEFT JOIN customer_order_items_agg coia
  ON co.transaction_id = coia.transaction_id
WHERE co.rn = 1
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ==================================================================================
# TARGET TABLE: silver.customer_order_items
# ==================================================================================

# Deduplicate bronze.customer_order_items on (transaction_id, product_id, transaction_time)
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_order_items_dedup AS
SELECT
  coi.*,
  ROW_NUMBER() OVER (
    PARTITION BY coi.transaction_id, coi.product_id, coi.transaction_time
    ORDER BY coi.transaction_time DESC
  ) AS rn
FROM customer_order_items_bronze coi
""")

customer_order_items_silver_df = spark.sql("""
SELECT
  -- Conformed keys
  CAST(TRIM(coi.transaction_id) AS STRING)                                   AS order_id,

  -- Stable surrogate id per UDT: HASH(transaction_id||'|'||product_id||'|'||CAST(transaction_time AS STRING))
  CAST(
    hash(
      concat(
        CAST(TRIM(coi.transaction_id) AS STRING),
        '|',
        CAST(TRIM(coi.product_id) AS STRING),
        '|',
        CAST(coi.transaction_time AS STRING)
      )
    ) AS STRING
  )                                                                          AS order_item_id,

  CAST(TRIM(coi.product_id) AS STRING)                                       AS product_id,

  -- Standardize quantity/amounts per UDT
  CAST(
    CASE WHEN CAST(coi.quantity AS INT) > 0 THEN CAST(coi.quantity AS INT) ELSE 0 END
    AS INT
  )                                                                          AS quantity,

  CAST(
    CASE
      WHEN coi.quantity IS NOT NULL AND CAST(coi.quantity AS INT) > 0
        THEN CAST(CAST(coi.sale_amount AS DECIMAL(18,2)) / CAST(coi.quantity AS DECIMAL(18,2)) AS DECIMAL(18,2))
      ELSE CAST(0 AS DECIMAL(18,2))
    END
    AS DECIMAL(18,2)
  )                                                                          AS unit_price_amount,

  CAST(CAST(coi.sale_amount AS DECIMAL(18,2)) AS DECIMAL(18,2))              AS line_total_amount
FROM customer_order_items_dedup coi
WHERE coi.rn = 1
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items.csv")
)

# ==================================================================================
# TARGET TABLE: silver.order_data_quality_summary
# ==================================================================================

order_data_quality_summary_silver_df = spark.sql("""
SELECT
  -- UDT: ingestion_date = CURRENT_DATE
  CAST(CURRENT_DATE AS DATE) AS ingestion_date,

  -- total_records = COUNT(*)
  CAST(COUNT(*) AS INT) AS total_records,

  -- invalid_records = SUM(CASE WHEN ... THEN 1 ELSE 0 END)
  CAST(SUM(
    CASE
      WHEN odqs.store_id_is_null
        OR odqs.product_id_is_null
        OR odqs.quantity_is_null
        OR odqs.quantity_is_invalid
        OR odqs.sale_amount_is_null
        OR odqs.sale_amount_is_invalid
        OR odqs.transaction_time_is_null
      THEN 1 ELSE 0
    END
  ) AS INT) AS invalid_records,

  -- valid_records = COUNT(*) - SUM(invalid_case)
  CAST(
    COUNT(*) - SUM(
      CASE
        WHEN odqs.store_id_is_null
          OR odqs.product_id_is_null
          OR odqs.quantity_is_null
          OR odqs.quantity_is_invalid
          OR odqs.sale_amount_is_null
          OR odqs.sale_amount_is_invalid
          OR odqs.transaction_time_is_null
        THEN 1 ELSE 0
      END
    )
    AS INT
  ) AS valid_records,

  -- data_quality_score_pct = ROUND(100.0 * valid_records / NULLIF(total_records,0), 2)
  CAST(
    ROUND(
      100.0 * (
        (COUNT(*) - SUM(
          CASE
            WHEN odqs.store_id_is_null
              OR odqs.product_id_is_null
              OR odqs.quantity_is_null
              OR odqs.quantity_is_invalid
              OR odqs.sale_amount_is_null
              OR odqs.sale_amount_is_invalid
              OR odqs.transaction_time_is_null
            THEN 1 ELSE 0
          END
        )) / NULLIF(COUNT(*), 0)
      ),
      2
    )
    AS DECIMAL(10,2)
  ) AS data_quality_score_pct
FROM order_data_quality_summary_bronze odqs
GROUP BY CAST(CURRENT_DATE AS DATE)
""")

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    order_data_quality_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_data_quality_summary.csv")
)

job.commit()
```