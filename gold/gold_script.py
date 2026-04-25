```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Ensure "single CSV file" output per target
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT PATH FORMAT)
# ------------------------------------------------------------------------------------
customer_orders_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_df.createOrReplaceTempView("customer_orders")

customer_order_line_items_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_items.{FILE_FORMAT}/")
)
customer_order_line_items_df.createOrReplaceTempView("customer_order_line_items")

etl_batch_run_metadata_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_batch_run_metadata.{FILE_FORMAT}/")
)
etl_batch_run_metadata_df.createOrReplaceTempView("etl_batch_run_metadata")

data_quality_results_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_results.{FILE_FORMAT}/")
)
data_quality_results_df.createOrReplaceTempView("data_quality_results")

customer_sensitive_access_map_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_sensitive_access_map.{FILE_FORMAT}/")
)
customer_sensitive_access_map_df.createOrReplaceTempView("customer_sensitive_access_map")

# ------------------------------------------------------------------------------------
# Target: gold.gold_customer_orders (gco) from silver.customer_orders (sco)
# ------------------------------------------------------------------------------------
gold_customer_orders_sql = """
SELECT
  CAST(sco.order_id AS STRING)            AS order_id,
  CAST(sco.order_date AS DATE)            AS order_date,
  CAST(sco.customer_id AS STRING)         AS customer_id,
  CAST(sco.order_status AS STRING)        AS order_status,
  CAST(sco.currency_code AS STRING)       AS currency_code,
  CAST(sco.order_total_amount AS DECIMAL(38, 18)) AS order_total_amount,
  CAST(sco.source_system AS STRING)       AS source_system,
  CAST(sco.ingestion_date AS DATE)        AS ingestion_date
FROM customer_orders sco
"""
gold_customer_orders_out_df = spark.sql(gold_customer_orders_sql)

(
    gold_customer_orders_out_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold.gold_customer_orders_line_items (gcoli) from silver.customer_order_line_items (scoli)
# ------------------------------------------------------------------------------------
gold_customer_orders_line_items_sql = """
SELECT
  CAST(scoli.order_id AS STRING)          AS order_id,
  CAST(scoli.order_line_id AS STRING)     AS order_line_id,
  CAST(scoli.product_id AS STRING)        AS product_id,
  CAST(scoli.quantity AS INT)             AS quantity,
  CAST(scoli.unit_price_amount AS DECIMAL(38, 18)) AS unit_price_amount,
  CAST(scoli.line_total_amount AS DECIMAL(38, 18)) AS line_total_amount,
  CAST(scoli.ingestion_date AS DATE)      AS ingestion_date
FROM customer_order_line_items scoli
"""
gold_customer_orders_line_items_out_df = spark.sql(gold_customer_orders_line_items_sql)

(
    gold_customer_orders_line_items_out_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders_line_items.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold.gold_etl_batch_run_metadata (gebrm) from silver.etl_batch_run_metadata (sebrm)
# ------------------------------------------------------------------------------------
gold_etl_batch_run_metadata_sql = """
SELECT
  CAST(sebrm.batch_id AS STRING)          AS batch_id,
  CAST(sebrm.pipeline_name AS STRING)     AS pipeline_name,
  CAST(sebrm.run_date AS DATE)            AS run_date,
  CAST(sebrm.source_s3_path AS STRING)    AS source_s3_path,
  CAST(sebrm.start_timestamp AS TIMESTAMP) AS start_timestamp,
  CAST(sebrm.end_timestamp AS TIMESTAMP)   AS end_timestamp,
  CAST(sebrm.records_read AS BIGINT)      AS records_read,
  CAST(sebrm.records_loaded AS BIGINT)    AS records_loaded,
  CAST(sebrm.run_status AS STRING)        AS run_status
FROM etl_batch_run_metadata sebrm
"""
gold_etl_batch_run_metadata_out_df = spark.sql(gold_etl_batch_run_metadata_sql)

(
    gold_etl_batch_run_metadata_out_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_etl_batch_run_metadata.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold.gold_data_quality_results (gdqr) from silver.data_quality_results (sdqr)
# ------------------------------------------------------------------------------------
gold_data_quality_results_sql = """
SELECT
  CAST(sdqr.batch_id AS STRING)           AS batch_id,
  CAST(sdqr.run_date AS DATE)             AS run_date,
  CAST(sdqr.check_name AS STRING)         AS check_name,
  CAST(sdqr.check_type AS STRING)         AS check_type,
  CAST(sdqr.target_table AS STRING)       AS target_table,
  CAST(sdqr.records_evaluated AS BIGINT)  AS records_evaluated,
  CAST(sdqr.records_failed AS BIGINT)     AS records_failed,
  CAST(sdqr.quality_score_pct AS DECIMAL(38, 18)) AS quality_score_pct,
  CAST(sdqr.result_status AS STRING)      AS result_status
FROM data_quality_results sdqr
"""
gold_data_quality_results_out_df = spark.sql(gold_data_quality_results_sql)

(
    gold_data_quality_results_out_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_data_quality_results.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold.gold_customer_sensitive_access_map (gcsam) from silver.customer_sensitive_access_map (scsam)
# ------------------------------------------------------------------------------------
gold_customer_sensitive_access_map_sql = """
SELECT
  CAST(scsam.customer_id AS STRING)       AS customer_id,
  CAST(scsam.is_sensitive AS BOOLEAN)     AS is_sensitive,
  CAST(scsam.masking_policy_name AS STRING) AS masking_policy_name,
  CAST(scsam.effective_start_date AS DATE) AS effective_start_date,
  CAST(scsam.effective_end_date AS DATE)   AS effective_end_date
FROM customer_sensitive_access_map scsam
"""
gold_customer_sensitive_access_map_out_df = spark.sql(gold_customer_sensitive_access_map_sql)

(
    gold_customer_sensitive_access_map_out_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_sensitive_access_map.csv")
)

job.commit()
```