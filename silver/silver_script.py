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

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV read options (adjust if your bronze differs)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# For single CSV output file
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# ===================================================================================
# TABLE: orders_silver
# ===================================================================================

# 1) Read source tables
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
orders_bronze_df.createOrReplaceTempView("orders_bronze")

# 3) SQL transformations (dedup to latest per order_id by order_timestamp DESC; tie-breaker: highest order_total_amount)
orders_silver_df = spark.sql("""
WITH ranked AS (
    SELECT
        ob.*,
        ROW_NUMBER() OVER (
            PARTITION BY ob.order_id
            ORDER BY
                ob.order_timestamp DESC,
                CAST(ob.order_total_amount AS DECIMAL(18,2)) DESC
        ) AS rn
    FROM orders_bronze ob
)
SELECT
    ob.order_id                                                        AS order_id,
    CAST(ob.order_timestamp AS DATE)                                    AS order_date,
    ob.store_id                                                         AS customer_id,
    'COMPLETED'                                                         AS order_status,
    CAST(ob.order_total_amount AS DECIMAL(18,2))                         AS order_total_amount,
    'USD'                                                               AS currency_code,
    'UNKNOWN'                                                           AS payment_method,
    'UNKNOWN'                                                           AS shipping_method,
    'UNKNOWN'                                                           AS ship_to_country_code,
    'sales_transactions_raw'                                            AS record_source,
    CURRENT_DATE                                                        AS ingestion_date
FROM ranked ob
WHERE ob.rn = 1
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ===================================================================================
# TABLE: order_items_silver
# ===================================================================================

# 1) Read source tables
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# 3) SQL transformations (filter invalids; deduplicate; deterministic hash ID; unit price; ingestion_date)
order_items_silver_df = spark.sql("""
WITH cleaned AS (
    SELECT
        oib.order_id                                                   AS order_id,
        oib.product_id                                                 AS product_id,
        CAST(oib.quantity AS INT)                                      AS quantity_int,
        CAST(oib.line_total_amount AS DECIMAL(18,2))                   AS line_total_amount_dec,
        oib.transaction_time                                           AS transaction_time
    FROM order_items_bronze oib
    WHERE
        CAST(oib.quantity AS INT) > 0
        AND oib.line_total_amount IS NOT NULL
),
deduped AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                c.order_id,
                c.product_id,
                c.transaction_time,
                c.quantity_int,
                c.line_total_amount_dec
            ORDER BY c.transaction_time DESC
        ) AS rn
    FROM cleaned c
)
SELECT
    d.order_id                                                                 AS order_id,
    CAST(
        hash(d.order_id, d.product_id, d.transaction_time, d.quantity_int, d.line_total_amount_dec)
        AS STRING
    )                                                                          AS order_item_id,
    d.product_id                                                               AS product_id,
    d.quantity_int                                                             AS quantity,
    ROUND(d.line_total_amount_dec / NULLIF(d.quantity_int, 0), 2)              AS unit_price_amount,
    d.line_total_amount_dec                                                    AS line_total_amount,
    CAST(d.transaction_time AS DATE)                                           AS ingestion_date
FROM deduped d
WHERE d.rn = 1
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ===================================================================================
# TABLE: daily_etl_run_metrics_silver
# ===================================================================================

# 1) Read source tables
daily_etl_run_metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/daily_etl_run_metrics_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
daily_etl_run_metrics_bronze_df.createOrReplaceTempView("daily_etl_run_metrics_bronze")

# 3) SQL transformations (aggregate by load_date; derive run window; counts; duration)
daily_etl_run_metrics_silver_df = spark.sql("""
SELECT
    dermb.load_date                                             AS run_date,
    'sales_transactions_raw'                                    AS dataset_name,
    COUNT(dermb.source_transaction_id)                          AS records_read_count,
    COUNT(dermb.source_transaction_id)                          AS records_loaded_count,
    CAST(0 AS BIGINT)                                           AS records_rejected_count,
    MIN(dermb.ingestion_timestamp)                              AS load_start_ts,
    MAX(dermb.ingestion_timestamp)                              AS load_end_ts,
    CAST(
        (unix_timestamp(MAX(dermb.ingestion_timestamp)) - unix_timestamp(MIN(dermb.ingestion_timestamp))) / 60
        AS INT
    )                                                           AS duration_minutes
FROM daily_etl_run_metrics_bronze dermb
GROUP BY dermb.load_date
""")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    daily_etl_run_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_etl_run_metrics_silver.csv")
)

job.commit()
```