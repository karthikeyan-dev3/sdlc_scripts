```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Job Setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    NOTE: Path format is STRICT: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

daily_order_kpis_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/daily_order_kpis_silver.{FILE_FORMAT}/")
)

data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
orders_silver_df.createOrReplaceTempView("orders_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")
daily_order_kpis_silver_df.createOrReplaceTempView("daily_order_kpis_silver")
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

# ====================================================================================
# TARGET TABLE: gold_customer_orders
# Mapping: silver.orders_silver os
# ====================================================================================
gold_customer_orders_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(os.order_id AS STRING)            AS order_id,
            CAST(os.customer_id AS STRING)         AS customer_id,
            CAST(os.order_date AS DATE)            AS order_date,
            CAST(os.order_status AS STRING)        AS order_status,
            CAST(os.currency_code AS STRING)       AS currency_code,
            CAST(os.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount
        FROM orders_silver os
    ),
    dedup AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.order_id
                ORDER BY b.order_date DESC
            ) AS rn
        FROM base b
    )
    SELECT
        order_id,
        customer_id,
        order_date,
        order_status,
        currency_code,
        order_total_amount
    FROM dedup
    WHERE rn = 1
    """
)

gold_customer_orders_output_path = f"{TARGET_PATH}/gold_customer_orders.csv"
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_output_path)
)

# ====================================================================================
# TARGET TABLE: gold_customer_order_items
# Mapping: silver.order_items_silver ois INNER JOIN silver.orders_silver os ON ois.order_id = os.order_id
# ====================================================================================
gold_customer_order_items_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(ois.order_id AS STRING)                 AS order_id,
            CAST(ois.order_item_id AS INT)               AS order_item_id,
            CAST(ois.product_id AS STRING)               AS product_id,
            CAST(ois.quantity AS INT)                    AS quantity,
            CAST(ois.unit_price_amount AS DECIMAL(38, 10)) AS unit_price_amount,
            CAST(ois.line_total_amount AS DECIMAL(38, 10)) AS line_total_amount
        FROM order_items_silver ois
        INNER JOIN orders_silver os
            ON ois.order_id = os.order_id
    ),
    dedup AS (
        SELECT
            j.*,
            ROW_NUMBER() OVER (
                PARTITION BY j.order_id, j.order_item_id
                ORDER BY j.order_id, j.order_item_id
            ) AS rn
        FROM joined j
    )
    SELECT
        order_id,
        order_item_id,
        product_id,
        quantity,
        unit_price_amount,
        line_total_amount
    FROM dedup
    WHERE rn = 1
    """
)

gold_customer_order_items_output_path = f"{TARGET_PATH}/gold_customer_order_items.csv"
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_order_items_output_path)
)

# ====================================================================================
# TARGET TABLE: gold_daily_order_kpis
# Mapping: silver.daily_order_kpis_silver doks
# ====================================================================================
gold_daily_order_kpis_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(doks.order_date AS DATE)               AS order_date,
            CAST(doks.total_orders AS INT)              AS total_orders,
            CAST(doks.total_customers AS INT)           AS total_customers,
            CAST(doks.gross_sales_amount AS DECIMAL(38, 10)) AS gross_sales_amount
        FROM daily_order_kpis_silver doks
    ),
    dedup AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.order_date
                ORDER BY b.order_date DESC
            ) AS rn
        FROM base b
    )
    SELECT
        order_date,
        total_orders,
        total_customers,
        gross_sales_amount
    FROM dedup
    WHERE rn = 1
    """
)

gold_daily_order_kpis_output_path = f"{TARGET_PATH}/gold_daily_order_kpis.csv"
(
    gold_daily_order_kpis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_daily_order_kpis_output_path)
)

# ====================================================================================
# TARGET TABLE: gold_data_quality_daily
# Mapping: silver.data_quality_daily_silver dqds
# ====================================================================================
gold_data_quality_daily_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(dqds.run_date AS DATE)                 AS run_date,
            CAST(dqds.source_record_count AS INT)       AS source_record_count,
            CAST(dqds.loaded_order_count AS INT)        AS loaded_order_count,
            CAST(dqds.loaded_order_item_count AS INT)   AS loaded_order_item_count,
            CAST(dqds.rejected_record_count AS INT)     AS rejected_record_count,
            CAST(dqds.accuracy_rate AS DECIMAL(38, 10)) AS accuracy_rate
        FROM data_quality_daily_silver dqds
    ),
    dedup AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.run_date
                ORDER BY b.run_date DESC
            ) AS rn
        FROM base b
    )
    SELECT
        run_date,
        source_record_count,
        loaded_order_count,
        loaded_order_item_count,
        rejected_record_count,
        accuracy_rate
    FROM dedup
    WHERE rn = 1
    """
)

gold_data_quality_daily_output_path = f"{TARGET_PATH}/gold_data_quality_daily.csv"
(
    gold_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_data_quality_daily_output_path)
)

job.commit()
```