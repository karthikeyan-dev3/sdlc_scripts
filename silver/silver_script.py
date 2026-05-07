import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------
# Read Source Tables (Bronze)
# --------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# =================================================================================
# TARGET TABLE: site_performance_silver
# =================================================================================
site_performance_silver_sql = """
WITH sb_dedup AS (
  SELECT
    store_id,
    store_name
  FROM (
    SELECT
      sb.*,
      ROW_NUMBER() OVER (
        PARTITION BY sb.store_id
        ORDER BY COALESCE(sb.store_name, '') DESC
      ) AS rn
    FROM stores_bronze sb
  ) t
  WHERE rn = 1
),
tx AS (
  SELECT
    store_id,
    COUNT(DISTINCT transaction_id) AS actual_enrollment,
    MAX(transaction_time) AS last_activity_ts
  FROM sales_transactions_bronze
  GROUP BY store_id
)
SELECT
  CAST(TRIM(sb.store_id) AS STRING) AS site_id,
  CAST(TRIM(sb.store_name) AS STRING) AS site_name,
  CAST(COALESCE(tx.actual_enrollment, 0) AS INT) AS actual_enrollment,
  CAST(tx.last_activity_ts AS TIMESTAMP) AS last_activity_ts
FROM sb_dedup sb
LEFT JOIN tx
  ON sb.store_id = tx.store_id
"""

site_performance_silver_df = spark.sql(site_performance_silver_sql)
site_performance_silver_df.createOrReplaceTempView("site_performance_silver")

(
    site_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_performance_silver.csv")
)

# =================================================================================
# TARGET TABLE: site_health_silver
# =================================================================================
site_health_silver_sql = """
SELECT
  CAST(TRIM(sps.site_id) AS STRING) AS site_id,
  CAST(DATE(sps.last_activity_ts) AS DATE) AS report_date,
  CAST(
    CASE
      WHEN sps.last_activity_ts IS NULL THEN 'INACTIVE'
      WHEN COALESCE(sps.actual_enrollment, 0) = 0 THEN 'AT_RISK'
      WHEN COALESCE(sps.actual_enrollment, 0) < 10 THEN 'ON_TRACK'
      ELSE 'HEALTHY'
    END AS STRING
  ) AS performance_status,
  CAST(
    TRIM(
      COALESCE(
        CONCAT(
          CASE WHEN sps.site_name IS NULL OR TRIM(sps.site_name) = '' THEN 'MISSING_SITE_NAME; ' ELSE '' END,
          CASE WHEN sps.last_activity_ts IS NULL THEN 'NO_LAST_ACTIVITY; ' ELSE '' END,
          CASE WHEN COALESCE(sps.actual_enrollment, 0) = 0 THEN 'ZERO_ACTIVITY; ' ELSE '' END
        ),
        ''
      )
    ) AS STRING
  ) AS issues_identified
FROM site_performance_silver sps
"""

site_health_silver_df = spark.sql(site_health_silver_sql)
site_health_silver_df.createOrReplaceTempView("site_health_silver")

(
    site_health_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_health_silver.csv")
)

# =================================================================================
# TARGET TABLE: data_validation_silver
# =================================================================================
data_validation_silver_sql = """
WITH stb_dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time
  FROM (
    SELECT
      stb.*,
      ROW_NUMBER() OVER (
        PARTITION BY stb.transaction_id
        ORDER BY COALESCE(stb.transaction_time, TIMESTAMP('1900-01-01 00:00:00')) DESC
      ) AS rn
    FROM sales_transactions_bronze stb
  ) t
  WHERE rn = 1
)
SELECT
  CAST(TRIM(stb.transaction_id) AS STRING) AS record_id,
  CAST(TRIM(stb.store_id) AS STRING) AS site_id,
  CAST(stb.transaction_time AS TIMESTAMP) AS run_timestamp,
  CAST(
    CASE
      WHEN sb.store_id IS NULL THEN 'FAIL'
      WHEN pb.product_id IS NULL THEN 'FAIL'
      WHEN stb.transaction_time IS NULL THEN 'FAIL'
      WHEN CAST(COALESCE(stb.quantity, 0) AS INT) <= 0 THEN 'FAIL'
      WHEN CAST(COALESCE(stb.sale_amount, 0) AS DOUBLE) < 0 THEN 'FAIL'
      ELSE 'PASS'
    END AS STRING
  ) AS validation_status,
  CAST(
    TRIM(
      COALESCE(
        CONCAT(
          CASE WHEN sb.store_id IS NULL THEN 'INVALID_STORE_ID; ' ELSE '' END,
          CASE WHEN pb.product_id IS NULL THEN 'INVALID_PRODUCT_ID; ' ELSE '' END,
          CASE WHEN stb.transaction_time IS NULL THEN 'MISSING_TRANSACTION_TIME; ' ELSE '' END,
          CASE WHEN CAST(COALESCE(stb.quantity, 0) AS INT) <= 0 THEN 'INVALID_QUANTITY; ' ELSE '' END,
          CASE WHEN CAST(COALESCE(stb.sale_amount, 0) AS DOUBLE) < 0 THEN 'INVALID_SALE_AMOUNT; ' ELSE '' END
        ),
        ''
      )
    ) AS STRING
  ) AS validation_error_details
FROM stb_dedup stb
LEFT JOIN stores_bronze sb
  ON stb.store_id = sb.store_id
LEFT JOIN products_bronze pb
  ON stb.product_id = pb.product_id
"""

data_validation_silver_df = spark.sql(data_validation_silver_sql)
data_validation_silver_df.createOrReplaceTempView("data_validation_silver")

(
    data_validation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_validation_silver.csv")
)

# =================================================================================
# TARGET TABLE: system_metrics_silver
# =================================================================================
system_metrics_silver_sql = """
WITH stb_dedup AS (
  SELECT
    transaction_id,
    transaction_time
  FROM (
    SELECT
      stb.*,
      ROW_NUMBER() OVER (
        PARTITION BY stb.transaction_id
        ORDER BY COALESCE(stb.transaction_time, TIMESTAMP('1900-01-01 00:00:00')) DESC
      ) AS rn
    FROM sales_transactions_bronze stb
  ) t
  WHERE rn = 1
),
metrics AS (
  SELECT
    COUNT(1) AS records_processed,
    MIN(transaction_time) AS process_start_time,
    MAX(transaction_time) AS process_end_time
  FROM stb_dedup
)
SELECT
  CAST(DATE(CURRENT_TIMESTAMP()) AS STRING) AS batch_id,
  CAST(metrics.process_start_time AS TIMESTAMP) AS process_start_time,
  CAST(metrics.process_end_time AS TIMESTAMP) AS process_end_time,
  CAST(metrics.records_processed AS BIGINT) AS records_processed,
  CAST(
    CASE
      WHEN metrics.records_processed IS NULL OR metrics.records_processed = 0 THEN 'NO_DATA'
      ELSE 'SUCCESS'
    END AS STRING
  ) AS completion_status,
  CAST(NULL AS STRING) AS record_id
FROM metrics
"""

system_metrics_silver_df = spark.sql(system_metrics_silver_sql)
system_metrics_silver_df.createOrReplaceTempView("system_metrics_silver")

(
    system_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/system_metrics_silver.csv")
)

job.commit()
