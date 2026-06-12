import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
stores_raw_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_raw_bronze.{FILE_FORMAT}/")
)

sales_transactions_raw_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_raw_bronze.{FILE_FORMAT}/")
)

products_raw_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_raw_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
stores_raw_bronze_df.createOrReplaceTempView("stores_raw_bronze")
sales_transactions_raw_bronze_df.createOrReplaceTempView("sales_transactions_raw_bronze")
products_raw_bronze_df.createOrReplaceTempView("products_raw_bronze")

# ------------------------------------------------------------------------------
# silver.silver_project
# ------------------------------------------------------------------------------
silver_project_df = spark.sql(
    """
SELECT DISTINCT
  CAST(srb.store_id AS STRING) AS project_id,
  TRIM(srb.store_name) AS project_name,
  'Unknown' AS project_owner,
  'Unknown' AS project_status,
  CAST(srb.open_date AS DATE) AS start_date,
  'Unknown' AS end_date
FROM stores_raw_bronze srb
WHERE srb.store_id IS NOT NULL
"""
)

(
    silver_project_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_project.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_sample
# ------------------------------------------------------------------------------
silver_sample_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.transaction_id AS STRING) AS sample_id,
  CAST(strb.store_id AS STRING) AS project_id,
  CAST(strb.transaction_id AS STRING) AS sample_external_id,
  'Unknown' AS sample_type,
  'Unknown' AS subject_id,
  CAST(strb.transaction_time AS TIMESTAMP) AS collection_ts,
  CAST(strb.transaction_time AS TIMESTAMP) AS received_ts
FROM sales_transactions_raw_bronze strb
WHERE strb.transaction_id IS NOT NULL
  AND strb.store_id IS NOT NULL
"""
)

(
    silver_sample_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sample.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_instrument
# ------------------------------------------------------------------------------
silver_instrument_df = spark.sql(
    """
SELECT DISTINCT
  CAST(prb.product_id AS STRING) AS instrument_id,
  CAST(prb.product_id AS STRING) AS machine_id,
  TRIM(prb.category) AS instrument_type,
  TRIM(prb.brand) AS manufacturer,
  TRIM(prb.product_name) AS model,
  'Unknown' AS serial_number,
  'Unknown' AS location,
  COALESCE(CAST(prb.is_active AS BOOLEAN), false) AS is_active
FROM products_raw_bronze prb
WHERE prb.product_id IS NOT NULL
"""
)

(
    silver_instrument_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_instrument.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_machine_project_map
# ------------------------------------------------------------------------------
silver_machine_project_map_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.product_id AS STRING) AS machine_id,
  CAST(strb.store_id AS STRING) AS project_id,
  CAST(strb.transaction_time AS TIMESTAMP) AS effective_start_ts,
  'Unknown' AS effective_end_ts,
  true AS is_current,
  'sales_transactions_raw_bronze' AS mapping_source
FROM sales_transactions_raw_bronze strb
WHERE strb.product_id IS NOT NULL
  AND strb.store_id IS NOT NULL
  AND strb.transaction_time IS NOT NULL
"""
)

(
    silver_machine_project_map_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_machine_project_map.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_experiment
# ------------------------------------------------------------------------------
silver_experiment_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.store_id AS STRING) AS experiment_id,
  CAST(strb.store_id AS STRING) AS project_id,
  TRIM(srb.store_name) AS experiment_name,
  TRIM(srb.store_type) AS experiment_type,
  'Unknown' AS protocol_id,
  'Unknown' AS planned_start_ts,
  'Unknown' AS planned_end_ts,
  'Unknown' AS created_by,
  'Unknown' AS created_at_ts
FROM sales_transactions_raw_bronze strb
INNER JOIN stores_raw_bronze srb
  ON strb.store_id = srb.store_id
WHERE strb.store_id IS NOT NULL
"""
)

(
    silver_experiment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_experiment.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_run
# ------------------------------------------------------------------------------
silver_run_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.transaction_id AS STRING) AS run_id,
  CAST(strb.store_id AS STRING) AS experiment_id,
  CAST(strb.product_id AS STRING) AS instrument_id,
  CAST(strb.product_id AS STRING) AS machine_id,
  CAST(strb.transaction_time AS TIMESTAMP) AS run_start_ts,
  CAST(strb.transaction_time AS TIMESTAMP) AS run_end_ts,
  'Unknown' AS run_status,
  'Unknown' AS data_version,
  'sales_transactions_raw_bronze' AS source_format
FROM sales_transactions_raw_bronze strb
WHERE strb.transaction_id IS NOT NULL
  AND strb.store_id IS NOT NULL
  AND strb.product_id IS NOT NULL
"""
)

(
    silver_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_run.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_experiment_observation
# ------------------------------------------------------------------------------
silver_experiment_observation_df = spark.sql(
    """
SELECT DISTINCT
  CONCAT(CAST(strb.transaction_id AS STRING), '-', CAST(strb.product_id AS STRING)) AS observation_id,
  CAST(strb.store_id AS STRING) AS project_id,
  CAST(strb.store_id AS STRING) AS experiment_id,
  CAST(strb.transaction_id AS STRING) AS run_id,
  CAST(strb.transaction_id AS STRING) AS sample_id,
  CAST(strb.product_id AS STRING) AS instrument_id,
  CAST(strb.product_id AS STRING) AS machine_id,
  TRIM(prb.category) AS instrument_type,
  TRIM(prb.category) AS assay_type,
  'sale_amount' AS metric_name,
  CAST(strb.sale_amount AS DOUBLE) AS metric_value,
  'Unknown' AS metric_unit,
  'Unknown' AS result_status,
  CAST(strb.transaction_time AS TIMESTAMP) AS observed_at_ts,
  CURRENT_TIMESTAMP AS ingested_at_ts,
  CURRENT_TIMESTAMP AS processed_at_ts
FROM sales_transactions_raw_bronze strb
INNER JOIN products_raw_bronze prb
  ON strb.product_id = prb.product_id
WHERE strb.transaction_id IS NOT NULL
  AND strb.store_id IS NOT NULL
  AND strb.product_id IS NOT NULL
  AND strb.transaction_time IS NOT NULL
"""
)

(
    silver_experiment_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_experiment_observation.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_pipeline_run_audit
# ------------------------------------------------------------------------------
silver_pipeline_run_audit_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.transaction_id AS STRING) AS pipeline_run_id,
  'bronze_to_silver_sales' AS pipeline_name,
  CAST(strb.transaction_time AS TIMESTAMP) AS run_start_ts,
  CAST(strb.transaction_time AS TIMESTAMP) AS run_end_ts,
  'Unknown' AS run_status,
  'Unknown' AS records_read,
  'Unknown' AS records_written,
  'Unknown' AS error_count,
  'Unknown' AS last_success_ts,
  'Unknown' AS trigger_type
FROM sales_transactions_raw_bronze strb
WHERE strb.transaction_id IS NOT NULL
  AND strb.transaction_time IS NOT NULL
"""
)

(
    silver_pipeline_run_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_pipeline_run_audit.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_data_quality_check_result
# ------------------------------------------------------------------------------
silver_data_quality_check_result_df = spark.sql(
    """
SELECT DISTINCT
  CONCAT(CAST(strb.transaction_id AS STRING), '-dq') AS dq_result_id,
  'sales_transactions_raw_bronze' AS dataset_name,
  'run' AS entity_type,
  CAST(strb.transaction_id AS STRING) AS entity_id,
  'not_null_keys' AS check_name,
  'completeness' AS check_category,
  'medium' AS check_severity,
  CASE
    WHEN strb.transaction_id IS NOT NULL
     AND strb.store_id IS NOT NULL
     AND strb.product_id IS NOT NULL
     AND strb.transaction_time IS NOT NULL
    THEN 'pass' ELSE 'fail'
  END AS check_status,
  CASE
    WHEN strb.transaction_id IS NOT NULL
     AND strb.store_id IS NOT NULL
     AND strb.product_id IS NOT NULL
     AND strb.transaction_time IS NOT NULL
    THEN 0 ELSE 1
  END AS failed_rule_count,
  1 AS total_rule_count,
  CASE
    WHEN strb.transaction_id IS NOT NULL
     AND strb.store_id IS NOT NULL
     AND strb.product_id IS NOT NULL
     AND strb.transaction_time IS NOT NULL
    THEN 1.0 ELSE 0.0
  END AS quality_score,
  CURRENT_TIMESTAMP AS checked_at_ts,
  CAST(strb.transaction_id AS STRING) AS run_id
FROM sales_transactions_raw_bronze strb
WHERE strb.transaction_id IS NOT NULL
"""
)

(
    silver_data_quality_check_result_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_data_quality_check_result.csv")
)

# ------------------------------------------------------------------------------
# silver.silver_kpi_ingestion_performance_hourly
# ------------------------------------------------------------------------------
silver_kpi_ingestion_performance_hourly_df = spark.sql(
    """
SELECT DISTINCT
  CAST(strb.transaction_time AS TIMESTAMP) AS kpi_hour_ts,
  TRIM(prb.category) AS instrument_type,
  CAST(strb.store_id AS STRING) AS project_id,
  'Unknown' AS avg_processing_latency_seconds,
  'Unknown' AS p95_processing_latency_seconds,
  'Unknown' AS data_freshness_minutes,
  'Unknown' AS successful_runs,
  'Unknown' AS failed_runs,
  'Unknown' AS sla_adherence_pct
FROM sales_transactions_raw_bronze strb
INNER JOIN products_raw_bronze prb
  ON strb.product_id = prb.product_id
WHERE strb.transaction_time IS NOT NULL
  AND strb.store_id IS NOT NULL
  AND strb.product_id IS NOT NULL
"""
)

(
    silver_kpi_ingestion_performance_hourly_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_kpi_ingestion_performance_hourly.csv")
)

job.commit()