import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables (S3) + create temp views
# --------------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

customer_order_line_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_items_silver.{FILE_FORMAT}/")
)
customer_order_line_items_silver_df.createOrReplaceTempView("customer_order_line_items_silver")

etl_batch_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_batch_quality_metrics_silver.{FILE_FORMAT}/")
)
etl_batch_quality_metrics_silver_df.createOrReplaceTempView("etl_batch_quality_metrics_silver")

# --------------------------------------------------------------------------------------
# TARGET: gold.gold_customer_orders (gco)
# Source: silver.customer_orders_silver (cos)
# --------------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(cos.order_id AS STRING) AS order_id,
            CAST(cos.order_date AS DATE) AS order_date,
            CAST(cos.order_timestamp AS TIMESTAMP) AS order_timestamp,
            CAST(cos.order_total_amount AS DECIMAL(38,10)) AS order_total_amount,
            cos.ingestion_date AS ingestion_date,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(cos.order_id AS STRING)
                ORDER BY CAST(cos.order_timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM customer_orders_silver cos
    )
    SELECT
        order_id,
        order_date,
        order_timestamp,
        order_total_amount,
        ingestion_date
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: gold.gold_customer_order_line_items (gcoli)
# Source: silver.customer_order_line_items_silver (colis)
# --------------------------------------------------------------------------------------
gold_customer_order_line_items_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(colis.order_id AS STRING) AS order_id,
            CAST(colis.line_item_id AS STRING) AS line_item_id,
            CAST(colis.product_id AS STRING) AS product_id,
            CAST(colis.quantity AS INT) AS quantity,
            CAST(colis.line_total_amount AS DECIMAL(38,10)) AS line_total_amount,
            colis.ingestion_date AS ingestion_date,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(colis.order_id AS STRING), CAST(colis.line_item_id AS STRING)
                ORDER BY colis.ingestion_date DESC
            ) AS rn
        FROM customer_order_line_items_silver colis
    )
    SELECT
        order_id,
        line_item_id,
        product_id,
        quantity,
        line_total_amount,
        ingestion_date
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_customer_order_line_items_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_order_line_items.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: gold.gold_etl_batch_quality_metrics (geqbqm)
# Source: silver.etl_batch_quality_metrics_silver (ebqms)
# --------------------------------------------------------------------------------------
gold_etl_batch_quality_metrics_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(ebqms.batch_date AS DATE) AS batch_date,
            ebqms.source_system AS source_system,
            CAST(ebqms.records_received AS BIGINT) AS records_received,
            CAST(ebqms.records_loaded AS BIGINT) AS records_loaded,
            ebqms.records_rejected AS records_rejected,
            ebqms.data_quality_score AS data_quality_score,
            CAST(ebqms.ingestion_start_ts AS TIMESTAMP) AS ingestion_start_ts,
            CAST(ebqms.ingestion_end_ts AS TIMESTAMP) AS ingestion_end_ts,
            ebqms.ingestion_duration_minutes AS ingestion_duration_minutes,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(ebqms.batch_date AS DATE), ebqms.source_system
                ORDER BY CAST(ebqms.ingestion_end_ts AS TIMESTAMP) DESC
            ) AS rn
        FROM etl_batch_quality_metrics_silver ebqms
    )
    SELECT
        batch_date,
        source_system,
        records_received,
        records_loaded,
        records_rejected,
        data_quality_score,
        ingestion_start_ts,
        ingestion_end_ts,
        ingestion_duration_minutes
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_etl_batch_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_etl_batch_quality_metrics.csv")
)

job.commit()
