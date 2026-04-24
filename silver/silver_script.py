```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Source reads (S3) + temp views
# NOTE: Path format is REQUIRED: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

order_validation_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_validation_bronze.{FILE_FORMAT}/")
)
order_validation_bronze_df.createOrReplaceTempView("order_validation_bronze")

# ===================================================================================
# TARGET TABLE: silver.customer_orders_silver
# - De-dupe: ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) = 1
# - Transformations: exact per UDT + defaults from description
# ===================================================================================
customer_orders_silver_sql = """
WITH deduped AS (
  SELECT
    cob.*,
    ROW_NUMBER() OVER (
      PARTITION BY cob.transaction_id
      ORDER BY cob.transaction_time DESC
    ) AS rn
  FROM customer_orders_bronze cob
)
SELECT
  -- UDT mappings
  CAST(cob.transaction_id AS STRING)                                      AS order_id,
  CAST(cob.transaction_time AS TIMESTAMP)                                 AS order_timestamp,
  CAST(cob.transaction_time AS DATE)                                      AS order_date,
  CAST(cob.product_id AS STRING)                                          AS product_id,
  CAST(cob.quantity AS INT)                                               AS quantity,
  CAST(CASE WHEN cob.quantity <> 0 THEN cob.sale_amount / cob.quantity END AS DECIMAL(38, 10))
                                                                          AS unit_price,
  CAST(cob.sale_amount AS DECIMAL(38, 10))                                AS order_total_amount,

  -- Defaults / lineage (per table description)
  CAST(NULL AS STRING)                                                    AS customer_id,
  CAST(0 AS DECIMAL(38, 10))                                              AS discount_amount,
  CAST(0 AS DECIMAL(38, 10))                                              AS tax_amount,
  CAST(0 AS DECIMAL(38, 10))                                              AS shipping_amount,
  CAST('USD' AS STRING)                                                   AS currency_code,
  CAST(NULL AS STRING)                                                    AS payment_method,
  CAST('UNKNOWN' AS STRING)                                               AS order_status,
  CAST('sales_transactions_raw' AS STRING)                                AS source_system,
  CAST(CURRENT_DATE() AS DATE)                                            AS ingest_date,
  CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)                                  AS load_timestamp
FROM deduped cob
WHERE cob.rn = 1
"""

customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ===================================================================================
# TARGET TABLE: silver.order_validation_daily_silver
# - Group by ingest_date = CAST(transaction_time AS DATE)
# - Transformations: exact per UDT + constants from description
# ===================================================================================
order_validation_daily_silver_sql = """
SELECT
  CAST(ovb.transaction_time AS DATE) AS ingest_date,

  CAST(COUNT(ovb.transaction_id) AS INT) AS records_received,

  CAST(
    MIN(
      CASE
        WHEN ovb.transaction_id IS NOT NULL
         AND ovb.store_id IS NOT NULL
         AND ovb.product_id IS NOT NULL
         AND ovb.quantity IS NOT NULL
         AND ovb.sale_amount IS NOT NULL
         AND ovb.transaction_time IS NOT NULL
        THEN 1 ELSE 0
      END
    ) AS INT
  ) AS schema_validation_passed,

  CAST(
    MIN(
      CASE
        WHEN ovb.quantity > 0 AND ovb.sale_amount >= 0
        THEN 1 ELSE 0
      END
    ) AS INT
  ) AS data_quality_passed,

  CAST(
    SUM(
      CASE
        WHEN NOT(
          ovb.transaction_id IS NOT NULL
          AND ovb.store_id IS NOT NULL
          AND ovb.product_id IS NOT NULL
          AND ovb.quantity IS NOT NULL
          AND ovb.sale_amount IS NOT NULL
          AND ovb.transaction_time IS NOT NULL
          AND ovb.quantity > 0
          AND ovb.sale_amount >= 0
        )
        THEN 1 ELSE 0
      END
    ) AS INT
  ) AS records_rejected,

  CAST(
    (COUNT(*) - SUM(
      CASE
        WHEN NOT(
          ovb.transaction_id IS NOT NULL
          AND ovb.store_id IS NOT NULL
          AND ovb.product_id IS NOT NULL
          AND ovb.quantity IS NOT NULL
          AND ovb.sale_amount IS NOT NULL
          AND ovb.transaction_time IS NOT NULL
          AND ovb.quantity > 0
          AND ovb.sale_amount >= 0
        )
        THEN 1 ELSE 0
      END
    )) AS INT
  ) AS records_loaded,

  CAST(
    CASE
      WHEN COUNT(*) = 0 THEN 0
      ELSE
        CAST(
          (COUNT(*) - SUM(
            CASE
              WHEN NOT(
                ovb.transaction_id IS NOT NULL
                AND ovb.store_id IS NOT NULL
                AND ovb.product_id IS NOT NULL
                AND ovb.quantity IS NOT NULL
                AND ovb.sale_amount IS NOT NULL
                AND ovb.transaction_time IS NOT NULL
                AND ovb.quantity > 0
                AND ovb.sale_amount >= 0
              )
              THEN 1 ELSE 0
            END
          )) AS DECIMAL(38, 10)
        ) / CAST(COUNT(*) AS DECIMAL(38, 10))
    END AS DECIMAL(38, 10)
  ) AS validation_pass_rate,

  CAST(MIN(ovb.transaction_time) AS TIMESTAMP) AS first_record_timestamp,
  CAST(MAX(ovb.transaction_time) AS TIMESTAMP) AS last_record_timestamp,
  CAST(MIN(ovb.transaction_time) AS TIMESTAMP) AS load_start_timestamp,
  CAST(MAX(ovb.transaction_time) AS TIMESTAMP) AS load_end_timestamp,

  CAST('sales_transactions_raw' AS STRING) AS source_system
FROM order_validation_bronze ovb
GROUP BY CAST(ovb.transaction_time AS DATE)
"""

order_validation_daily_silver_df = spark.sql(order_validation_daily_silver_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    order_validation_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_validation_daily_silver.csv")
)

job.commit()
```