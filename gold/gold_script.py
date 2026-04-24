```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue job bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV options (adjust if your silver layer differs)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ===================================================================================
# 1) Read source tables from S3 + create temp views
# ===================================================================================

# silver.customer_orders
df_customer_orders = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
df_customer_orders.createOrReplaceTempView("customer_orders")

# silver.customer_order_items
df_customer_order_items = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)
df_customer_order_items.createOrReplaceTempView("customer_order_items")

# silver.etl_load_audit
df_etl_load_audit = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_load_audit.{FILE_FORMAT}/")
)
df_etl_load_audit.createOrReplaceTempView("etl_load_audit")

# ===================================================================================
# 2) TARGET: gold.gold_customer_orders
#    Mapping: silver.customer_orders sco
#             LEFT JOIN silver.etl_load_audit sela
#               ON sela.dataset_name = 'sales_transactions_raw'
#              AND sela.ingestion_date = sco.ingestion_date
#    Transform: load_timestamp = COALESCE(sela.processing_end_ts, sco.load_timestamp)
#    Dedup: apply ROW_NUMBER (keep latest by load_timestamp)
# ===================================================================================

sql_gold_customer_orders = """
WITH base AS (
    SELECT
        CAST(sco.order_id AS STRING)                            AS order_id,
        CAST(sco.order_date AS DATE)                            AS order_date,
        CAST(sco.customer_id AS STRING)                         AS customer_id,
        CAST(sco.order_status AS STRING)                        AS order_status,
        CAST(sco.order_total_amount AS DECIMAL(38, 10))         AS order_total_amount,
        CAST(sco.currency_code AS STRING)                       AS currency_code,
        CAST(sco.ingestion_date AS DATE)                        AS ingestion_date,
        CAST(sco.source_file_path AS STRING)                    AS source_file_path,
        CAST(COALESCE(sela.processing_end_ts, sco.load_timestamp) AS TIMESTAMP) AS load_timestamp
    FROM customer_orders sco
    LEFT JOIN etl_load_audit sela
        ON sela.dataset_name = 'sales_transactions_raw'
       AND sela.ingestion_date = sco.ingestion_date
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY load_timestamp DESC
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
    ingestion_date,
    source_file_path,
    load_timestamp
FROM dedup
WHERE rn = 1
"""

df_gold_customer_orders = spark.sql(sql_gold_customer_orders)

(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# 3) TARGET: gold.gold_customer_order_items
#    Mapping: silver.customer_order_items scoi
#             INNER JOIN silver.customer_orders sco ON sco.order_id = scoi.order_id
# ===================================================================================

sql_gold_customer_order_items = """
SELECT
    CAST(scoi.order_id AS STRING)                    AS order_id,
    CAST(scoi.order_item_id AS BIGINT)               AS order_item_id,
    CAST(scoi.product_id AS STRING)                  AS product_id,
    CAST(scoi.quantity AS INT)                       AS quantity,
    CAST(scoi.unit_price AS DECIMAL(38, 10))         AS unit_price,
    CAST(scoi.item_total_amount AS DECIMAL(38, 10))  AS item_total_amount
FROM customer_order_items scoi
INNER JOIN customer_orders sco
    ON sco.order_id = scoi.order_id
"""

df_gold_customer_order_items = spark.sql(sql_gold_customer_order_items)

(
    df_gold_customer_order_items.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# 4) TARGET: gold.gold_etl_load_audit
#    Mapping: silver.etl_load_audit sela (no joins)
# ===================================================================================

sql_gold_etl_load_audit = """
SELECT
    CAST(sela.pipeline_run_id AS STRING)          AS pipeline_run_id,
    CAST(sela.dataset_name AS STRING)             AS dataset_name,
    CAST(sela.ingestion_date AS DATE)             AS ingestion_date,
    CAST(sela.source_system AS STRING)            AS source_system,
    CAST(sela.records_received AS BIGINT)         AS records_received,
    CAST(sela.records_loaded AS BIGINT)           AS records_loaded,
    CAST(sela.records_rejected AS BIGINT)         AS records_rejected,
    CAST(sela.validation_status AS STRING)        AS validation_status,
    CAST(sela.processing_start_ts AS TIMESTAMP)   AS processing_start_ts,
    CAST(sela.processing_end_ts AS TIMESTAMP)     AS processing_end_ts
FROM etl_load_audit sela
"""

df_gold_etl_load_audit = spark.sql(sql_gold_etl_load_audit)

(
    df_gold_etl_load_audit.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_load_audit.csv")
)

job.commit()
```