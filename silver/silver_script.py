```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
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

# For Spark CSV reads (common for bronze CSVs)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ===================================================================================
# TABLE: orders_silver
# 1) Read source tables
# ===================================================================================
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

# 2) Create temp views (already created above)
# 3) Transform via Spark SQL (dedup latest order_timestamp per order_id)
orders_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            ob.*,
            ROW_NUMBER() OVER (
                PARTITION BY ob.order_id
                ORDER BY ob.order_timestamp DESC
            ) AS rn
        FROM orders_bronze ob
    )
    SELECT
        -- os.order_id = ob.order_id
        CAST(ob.order_id AS STRING) AS order_id,

        -- os.order_date = CAST(ob.order_timestamp AS DATE)
        CAST(ob.order_timestamp AS DATE) AS order_date,

        -- os.ingestion_date = CAST(ob.order_timestamp AS DATE)
        CAST(ob.order_timestamp AS DATE) AS ingestion_date,

        -- os.customer_id = ob.store_id
        CAST(ob.store_id AS STRING) AS customer_id,

        -- os.order_total_amount = CASE WHEN ob.order_total_amount >= 0 THEN ob.order_total_amount ELSE 0 END
        CAST(
            CASE
                WHEN CAST(ob.order_total_amount AS DECIMAL(18,2)) >= 0 THEN CAST(ob.order_total_amount AS DECIMAL(18,2))
                ELSE CAST(0 AS DECIMAL(18,2))
            END AS DECIMAL(18,2)
        ) AS order_total_amount,

        -- os.order_status = 'COMPLETED'
        CAST('COMPLETED' AS STRING) AS order_status,

        -- os.currency_code = 'USD'
        CAST('USD' AS STRING) AS currency_code
    FROM ranked ob
    WHERE ob.rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

# ===================================================================================
# TABLE: order_items_silver
# 1) Read source tables
# ===================================================================================
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# 2) Create temp views (already created above)
# 3) Transform via Spark SQL (dedup latest line_timestamp per (order_id, product_id))
order_items_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            oib.*,
            ROW_NUMBER() OVER (
                PARTITION BY oib.order_id, oib.product_id
                ORDER BY oib.line_timestamp DESC
            ) AS rn
        FROM order_items_bronze oib
    )
    SELECT
        -- ois.order_id = oib.order_id
        CAST(oib.order_id AS STRING) AS order_id,

        -- ois.product_id = oib.product_id
        CAST(oib.product_id AS STRING) AS product_id,

        -- ois.quantity = CASE WHEN oib.quantity > 0 THEN oib.quantity ELSE 1 END
        CAST(
            CASE
                WHEN CAST(oib.quantity AS INT) > 0 THEN CAST(oib.quantity AS INT)
                ELSE 1
            END AS INT
        ) AS quantity,

        -- ois.line_total_amount = CASE WHEN oib.line_amount >= 0 THEN oib.line_amount ELSE 0 END
        CAST(
            CASE
                WHEN CAST(oib.line_amount AS DECIMAL(18,2)) >= 0 THEN CAST(oib.line_amount AS DECIMAL(18,2))
                ELSE CAST(0 AS DECIMAL(18,2))
            END AS DECIMAL(18,2)
        ) AS line_total_amount,

        -- ois.unit_price = CASE WHEN oib.quantity > 0 THEN (oib.line_amount / oib.quantity) ELSE 0 END
        CAST(
            CASE
                WHEN CAST(oib.quantity AS INT) > 0
                    THEN CAST(oib.line_amount AS DECIMAL(18,6)) / CAST(oib.quantity AS DECIMAL(18,6))
                ELSE CAST(0 AS DECIMAL(18,6))
            END AS DECIMAL(18,6)
        ) AS unit_price,

        -- ois.order_item_id = MD5(oib.order_id || '|' || oib.product_id || '|' || CAST(oib.line_timestamp AS VARCHAR))
        md5(
            concat(
                CAST(oib.order_id AS STRING),
                '|',
                CAST(oib.product_id AS STRING),
                '|',
                CAST(oib.line_timestamp AS STRING)
            )
        ) AS order_item_id
    FROM ranked oib
    WHERE oib.rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

# ===================================================================================
# TABLE: etl_data_quality_daily_silver
# 1) Read source tables
# ===================================================================================
dq_daily_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_daily_bronze.{FILE_FORMAT}/")
)
dq_daily_bronze_df.createOrReplaceTempView("etl_data_quality_daily_bronze")

# 2) Create temp views (already created above)
# 3) Transform via Spark SQL
#    - De-duplicate identical events on (source_system, record_id, load_timestamp)
#    - Aggregate by dq_event_date and source_system
etl_data_quality_daily_silver_df = spark.sql(
    """
    WITH dedup_events AS (
        SELECT
            dqdb.*,
            ROW_NUMBER() OVER (
                PARTITION BY dqdb.source_system, dqdb.record_id, dqdb.load_timestamp
                ORDER BY dqdb.load_timestamp DESC
            ) AS rn
        FROM etl_data_quality_daily_bronze dqdb
    ),
    base AS (
        SELECT
            dqdb.dq_event_date,
            dqdb.source_system,
            dqdb.dq_pass_flag,
            dqdb.load_timestamp
        FROM dedup_events dqdb
        WHERE dqdb.rn = 1
    )
    SELECT
        -- dqds.ingestion_date = dqdb.dq_event_date
        CAST(base.dq_event_date AS DATE) AS ingestion_date,

        -- dqds.source_system = dqdb.source_system
        CAST(base.source_system AS STRING) AS source_system,

        -- dqds.total_records_ingested = COUNT(1)
        CAST(COUNT(1) AS BIGINT) AS total_records_ingested,

        -- dqds.total_records_valid = SUM(CASE WHEN dqdb.dq_pass_flag = 1 THEN 1 ELSE 0 END)
        CAST(SUM(CASE WHEN CAST(base.dq_pass_flag AS INT) = 1 THEN 1 ELSE 0 END) AS BIGINT) AS total_records_valid,

        -- dqds.total_records_rejected = COUNT(1) - SUM(CASE WHEN dqdb.dq_pass_flag = 1 THEN 1 ELSE 0 END)
        CAST(
            COUNT(1) - SUM(CASE WHEN CAST(base.dq_pass_flag AS INT) = 1 THEN 1 ELSE 0 END)
            AS BIGINT
        ) AS total_records_rejected,

        -- dqds.accuracy_rate_pct = (SUM(valid) * 100.0 / NULLIF(COUNT(1),0))
        CAST(
            (SUM(CASE WHEN CAST(base.dq_pass_flag AS INT) = 1 THEN 1 ELSE 0 END) * 100.0)
            / NULLIF(COUNT(1), 0)
            AS DECIMAL(18,4)
        ) AS accuracy_rate_pct,

        -- dqds.pipeline_start_ts = MIN(dqdb.load_timestamp)
        MIN(base.load_timestamp) AS pipeline_start_ts,

        -- dqds.pipeline_end_ts = MAX(dqdb.load_timestamp)
        MAX(base.load_timestamp) AS pipeline_end_ts,

        -- dqds.processing_time_minutes = DATEDIFF(minute, MIN(load_timestamp), MAX(load_timestamp))
        CAST(
            (unix_timestamp(MAX(base.load_timestamp)) - unix_timestamp(MIN(base.load_timestamp))) / 60
            AS INT
        ) AS processing_time_minutes
    FROM base
    GROUP BY
        base.dq_event_date,
        base.source_system
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    etl_data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_data_quality_daily_silver.csv")
)

job.commit()
```