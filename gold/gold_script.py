```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Recommended for CSV I/O
spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "true")

# ---------------------------------------------------------------------
# Source Reads (S3) + Temp Views
# NOTE: Paths must be constructed as: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ---------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

etl_run_audit_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/etl_run_audit.{FILE_FORMAT}/")
)
etl_run_audit_silver_df.createOrReplaceTempView("etl_run_audit_silver")

data_quality_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/data_quality_results.{FILE_FORMAT}/")
)
data_quality_results_silver_df.createOrReplaceTempView("data_quality_results_silver")

# ---------------------------------------------------------------------
# Target: gold.gold_customer_orders
# Mapping: silver.customer_orders sco
# ---------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sco.order_id AS STRING)                            AS order_id,
            CAST(sco.order_date AS DATE)                            AS order_date,
            CAST(sco.customer_id AS STRING)                         AS customer_id,
            CAST(sco.order_status AS STRING)                        AS order_status,
            CAST(sco.order_total_amount AS DECIMAL(38, 10))         AS order_total_amount,
            CAST(sco.currency_code AS STRING)                       AS currency_code,
            CAST(sco.ingestion_date AS DATE)                        AS ingestion_date
        FROM customer_orders_silver sco
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        order_total_amount,
        currency_code,
        ingestion_date
    FROM base
    """
)

# Write SINGLE CSV under TARGET_PATH (no subfolders) => TARGET_PATH + "/gold_customer_orders.csv"
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ---------------------------------------------------------------------
# Target: gold.gold_customer_order_items
# Mapping: silver.customer_order_items scoi
#          INNER JOIN silver.customer_orders sco ON scoi.order_id = sco.order_id
# ---------------------------------------------------------------------
gold_customer_order_items_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            scoi.*
        FROM customer_order_items_silver scoi
        INNER JOIN customer_orders_silver sco
            ON CAST(scoi.order_id AS STRING) = CAST(sco.order_id AS STRING)
    ),
    base AS (
        SELECT
            CAST(j.order_id AS STRING)                              AS order_id,
            CAST(j.order_item_id AS STRING)                         AS order_item_id,
            CAST(j.product_id AS STRING)                            AS product_id,
            CAST(j.quantity AS INT)                                 AS quantity,
            CAST(j.unit_price AS DECIMAL(38, 10))                   AS unit_price,
            CAST(j.line_total_amount AS DECIMAL(38, 10))            AS line_total_amount
        FROM joined j
    )
    SELECT
        order_id,
        order_item_id,
        product_id,
        quantity,
        unit_price,
        line_total_amount
    FROM base
    """
)

# Write SINGLE CSV under TARGET_PATH (no subfolders) => TARGET_PATH + "/gold_customer_order_items.csv"
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ---------------------------------------------------------------------
# Target: gold.gold_etl_run_audit
# Mapping: silver.etl_run_audit sera
# ---------------------------------------------------------------------
gold_etl_run_audit_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sera.pipeline_name AS STRING)                      AS pipeline_name,
            CAST(sera.run_id AS STRING)                             AS run_id,
            CAST(sera.run_date AS DATE)                             AS run_date,
            CAST(sera.status AS STRING)                             AS status,
            CAST(sera.records_read AS BIGINT)                       AS records_read,
            CAST(sera.records_loaded AS BIGINT)                     AS records_loaded,
            CAST(sera.error_count AS INT)                           AS error_count,
            CAST(sera.started_at AS TIMESTAMP)                      AS started_at,
            CAST(sera.ended_at AS TIMESTAMP)                        AS ended_at
        FROM etl_run_audit_silver sera
    )
    SELECT
        pipeline_name,
        run_id,
        run_date,
        status,
        records_read,
        records_loaded,
        error_count,
        started_at,
        ended_at
    FROM base
    """
)

# Write SINGLE CSV under TARGET_PATH (no subfolders) => TARGET_PATH + "/gold_etl_run_audit.csv"
(
    gold_etl_run_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_run_audit.csv")
)

# ---------------------------------------------------------------------
# Target: gold.gold_data_quality_results
# Mapping: silver.data_quality_results sdqr
#          INNER JOIN silver.etl_run_audit sera ON sdqr.run_id = sera.run_id
# ---------------------------------------------------------------------
gold_data_quality_results_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            sdqr.*
        FROM data_quality_results_silver sdqr
        INNER JOIN etl_run_audit_silver sera
            ON CAST(sdqr.run_id AS STRING) = CAST(sera.run_id AS STRING)
    ),
    base AS (
        SELECT
            CAST(j.run_id AS STRING)                                AS run_id,
            CAST(j.check_name AS STRING)                            AS check_name,
            CAST(j.check_status AS STRING)                          AS check_status,
            CAST(j.failed_record_count AS BIGINT)                   AS failed_record_count,
            CAST(j.check_timestamp AS TIMESTAMP)                    AS check_timestamp
        FROM joined j
    )
    SELECT
        run_id,
        check_name,
        check_status,
        failed_record_count,
        check_timestamp
    FROM base
    """
)

# Write SINGLE CSV under TARGET_PATH (no subfolders) => TARGET_PATH + "/gold_data_quality_results.csv"
(
    gold_data_quality_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_results.csv")
)

job.commit()
```