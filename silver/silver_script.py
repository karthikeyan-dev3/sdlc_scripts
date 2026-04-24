```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue boilerplate
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ====================================================================================
# SOURCE: bronze.customer_orders_bronze  -> TARGET: silver.customer_orders_silver
# ====================================================================================

# 1) Read source table
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

# 3) Transform (Spark SQL) + de-dup (latest transaction_time per order_id)
customer_orders_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            -- Transformations (EXACT per UDT)
            TRIM(cob.transaction_id)                                              AS order_id,
            CAST(cob.transaction_time AS DATE)                                     AS order_date,
            TRIM(cob.store_id)                                                     AS customer_id,
            CASE
                WHEN cob.transaction_id IS NOT NULL AND cob.transaction_time IS NOT NULL
                    THEN 'COMPLETED'
                ELSE 'UNKNOWN'
            END                                                                    AS order_status,
            CASE WHEN cob.sale_amount < 0 THEN NULL ELSE cob.sale_amount END       AS order_total_amount,
            'USD'                                                                  AS currency_code,

            cob.transaction_time                                                   AS transaction_time,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(cob.transaction_id)
                ORDER BY cob.transaction_time DESC
            )                                                                      AS rn
        FROM customer_orders_bronze cob
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        order_total_amount,
        currency_code
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ====================================================================================
# SOURCE: bronze.order_items_bronze  -> TARGET: silver.order_items_silver
# ====================================================================================

# 1) Read source table
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# 2) Create temp view
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# 3) Transform (Spark SQL) + de-dup (latest transaction_time per (order_id, product_id))
order_items_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            -- Transformations (EXACT per UDT)
            TRIM(oib.transaction_id)                                                                 AS order_id,
            CONCAT(TRIM(oib.transaction_id), '-', COALESCE(TRIM(oib.product_id), 'UNKNOWN'))          AS order_item_id,
            TRIM(oib.product_id)                                                                      AS product_id,
            CASE WHEN oib.quantity <= 0 THEN NULL ELSE oib.quantity END                               AS quantity,
            CASE
                WHEN oib.quantity IS NOT NULL AND oib.quantity <> 0 AND oib.quantity > 0
                     AND oib.sale_amount IS NOT NULL AND oib.sale_amount >= 0
                    THEN oib.sale_amount / oib.quantity
                ELSE NULL
            END                                                                                       AS unit_price,
            CASE WHEN oib.sale_amount < 0 THEN NULL ELSE oib.sale_amount END                          AS line_amount,

            oib.transaction_time                                                                       AS transaction_time,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(oib.transaction_id), TRIM(oib.product_id)
                ORDER BY oib.transaction_time DESC
            )                                                                                          AS rn
        FROM order_items_bronze oib
    )
    SELECT
        order_id,
        order_item_id,
        product_id,
        quantity,
        unit_price,
        line_amount
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

# ====================================================================================
# SOURCE: bronze.data_quality_daily_bronze  -> TARGET: silver.data_quality_daily_silver
# ====================================================================================

# 1) Read source table
data_quality_daily_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_bronze.{FILE_FORMAT}/")
)

# 2) Create temp view
data_quality_daily_bronze_df.createOrReplaceTempView("data_quality_daily_bronze")

# 3) Transform (Spark SQL) aggregated DQ metrics (one row per (load_date, source_file_date))
data_quality_daily_silver_df = spark.sql(
    """
    SELECT
        -- Transformations (EXACT per UDT)
        CAST(dqdb.transaction_time AS DATE)                                                                 AS load_date,
        CAST(dqdb.transaction_time AS DATE)                                                                 AS source_file_date,
        COUNT(1)                                                                                            AS records_ingested,
        SUM(
            CASE
                WHEN dqdb.transaction_id IS NULL
                  OR dqdb.transaction_time IS NULL
                  OR dqdb.sale_amount IS NULL OR dqdb.sale_amount < 0
                  OR dqdb.quantity IS NULL OR dqdb.quantity <= 0
                THEN 1 ELSE 0
            END
        )                                                                                                   AS records_rejected,
        (
            COUNT(1) - SUM(
                CASE
                    WHEN dqdb.transaction_id IS NULL
                      OR dqdb.transaction_time IS NULL
                      OR dqdb.sale_amount IS NULL OR dqdb.sale_amount < 0
                      OR dqdb.quantity IS NULL OR dqdb.quantity <= 0
                    THEN 1 ELSE 0
                END
            )
        )                                                                                                   AS records_loaded,
        CASE
            WHEN COUNT(1) = 0 THEN 0
            ELSE (
                (COUNT(1) - SUM(
                    CASE
                        WHEN dqdb.transaction_id IS NULL
                          OR dqdb.transaction_time IS NULL
                          OR dqdb.sale_amount IS NULL OR dqdb.sale_amount < 0
                          OR dqdb.quantity IS NULL OR dqdb.quantity <= 0
                        THEN 1 ELSE 0
                    END
                )) / COUNT(1)
            )
        END                                                                                                 AS dq_pass_rate,
        MIN(dqdb.transaction_time)                                                                          AS load_start_ts,
        MAX(dqdb.transaction_time)                                                                          AS load_end_ts
    FROM data_quality_daily_bronze dqdb
    GROUP BY
        CAST(dqdb.transaction_time AS DATE),
        CAST(dqdb.transaction_time AS DATE)
    """
)

# 4) Save output as SINGLE CSV file directly under TARGET_PATH
(
    data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_silver.csv")
)
```