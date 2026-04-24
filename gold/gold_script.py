```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
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
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ===================================================================================
# TABLE: gold.gold_customer_orders
# Sources: silver.customer_orders (alias: sco)
# ===================================================================================

# 1) Read source tables from S3
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

# 2) Create temp views
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# 3) SQL transformation (exact mapping)
df_gold_customer_orders = spark.sql(
    """
    SELECT
        CAST(sco.order_id AS STRING)              AS order_id,
        CAST(sco.order_date AS DATE)              AS order_date,
        CAST(sco.customer_id AS STRING)           AS customer_id,
        CAST(sco.order_status AS STRING)          AS order_status,
        CAST(sco.currency_code AS STRING)         AS currency_code,
        CAST(sco.order_total_amount AS DECIMAL(38, 18)) AS order_total_amount,
        CAST(sco.item_count AS INT)               AS item_count
    FROM customer_orders_silver sco
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TABLE: gold.gold_customer_order_line_items
# Sources: silver.customer_order_line_items (alias: scoli)
# ===================================================================================

# 1) Read source tables from S3
df_customer_order_line_items_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_items.{FILE_FORMAT}/")
)

# 2) Create temp views
df_customer_order_line_items_silver.createOrReplaceTempView("customer_order_line_items_silver")

# 3) SQL transformation (exact mapping)
df_gold_customer_order_line_items = spark.sql(
    """
    SELECT
        CAST(scoli.order_id AS STRING)                 AS order_id,
        CAST(scoli.line_item_id AS BIGINT)             AS line_item_id,
        CAST(scoli.product_id AS STRING)               AS product_id,
        CAST(scoli.quantity AS INT)                    AS quantity,
        CAST(scoli.unit_price_amount AS DECIMAL(38, 18)) AS unit_price_amount,
        CAST(scoli.line_total_amount AS DECIMAL(38, 18)) AS line_total_amount
    FROM customer_order_line_items_silver scoli
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_order_line_items.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_line_items.csv")
)

# ===================================================================================
# TABLE: gold.gold_etl_audit_lineage
# Sources: silver.etl_audit_lineage (alias: seal)
# ===================================================================================

# 1) Read source tables from S3
df_etl_audit_lineage_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_audit_lineage.{FILE_FORMAT}/")
)

# 2) Create temp views
df_etl_audit_lineage_silver.createOrReplaceTempView("etl_audit_lineage_silver")

# 3) SQL transformation (exact mapping)
df_gold_etl_audit_lineage = spark.sql(
    """
    SELECT
        CAST(seal.load_date AS DATE)             AS load_date,
        CAST(seal.source_system AS STRING)       AS source_system,
        CAST(seal.source_s3_path AS STRING)      AS source_s3_path,
        CAST(seal.target_table_name AS STRING)   AS target_table_name,
        CAST(seal.records_read AS BIGINT)        AS records_read,
        CAST(seal.records_loaded AS BIGINT)      AS records_loaded,
        CAST(seal.records_rejected AS BIGINT)    AS records_rejected,
        CAST(seal.dq_error_count AS BIGINT)      AS dq_error_count,
        CAST(seal.etl_run_id AS STRING)          AS etl_run_id,
        CAST(seal.etl_start_ts AS TIMESTAMP)     AS etl_start_ts,
        CAST(seal.etl_end_ts AS TIMESTAMP)       AS etl_end_ts
    FROM etl_audit_lineage_silver seal
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_etl_audit_lineage.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_audit_lineage.csv")
)

job.commit()
```