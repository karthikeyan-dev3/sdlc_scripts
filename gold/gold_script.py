import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Read source tables (S3)
# --------------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

etl_data_quality_results_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_results_silver.{FILE_FORMAT}/")
)
etl_data_quality_results_silver_df.createOrReplaceTempView("etl_data_quality_results_silver")

# --------------------------------------------------------------------------------------
# Target: gold.gold_customer_orders
# Mapping: customer_orders_silver cos LEFT JOIN aggregated customer_order_items_silver
# --------------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    WITH cois_agg AS (
        SELECT
            CAST(order_id AS STRING) AS order_id,
            COUNT(*) AS item_count
        FROM customer_order_items_silver
        GROUP BY CAST(order_id AS STRING)
    )
    SELECT
        CAST(cos.order_id AS STRING) AS order_id,
        CAST(cos.order_date AS DATE) AS order_date,
        CAST(cos.order_total_amount AS DECIMAL(18,2)) AS order_total_amount,
        CAST(cois_agg.item_count AS INT) AS item_count,
        CAST(cos.source_system AS STRING) AS source_system,
        CAST(cos.ingestion_date AS DATE) AS ingestion_date
    FROM customer_orders_silver cos
    LEFT JOIN cois_agg
        ON CAST(cos.order_id AS STRING) = cois_agg.order_id
    """
)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold.gold_customer_order_items
# Mapping: customer_order_items_silver cois
# --------------------------------------------------------------------------------------
gold_customer_order_items_df = spark.sql(
    """
    SELECT
        CAST(cois.order_id AS STRING) AS order_id,
        CAST(cois.order_item_id AS STRING) AS order_item_id,
        CAST(cois.product_id AS STRING) AS product_id,
        CAST(cois.quantity AS INT) AS quantity,
        CAST(cois.unit_price_amount AS DECIMAL(18,2)) AS unit_price_amount,
        CAST(cois.line_total_amount AS DECIMAL(18,2)) AS line_total_amount
    FROM customer_order_items_silver cois
    """
)

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold.gold_etl_data_quality_results
# Mapping: aggregated etl_data_quality_results_silver dqrs
# --------------------------------------------------------------------------------------
gold_etl_data_quality_results_df = spark.sql(
    """
    SELECT
        CAST(dqrs.detected_at AS DATE) AS run_date,
        CAST(dqrs.source_table AS STRING) AS dataset_name,
        CAST(COUNT(DISTINCT dqrs.record_id) AS BIGINT) AS total_records,
        CAST(
            COUNT(DISTINCT CASE
                WHEN dqrs.record_id IS NOT NULL
                 AND dqrs.record_id NOT IN (
                    SELECT record_id
                    FROM etl_data_quality_results_silver
                    WHERE CAST(detected_at AS DATE) = CAST(dqrs.detected_at AS DATE)
                      AND source_table = dqrs.source_table
                      AND check_status = 'FAIL'
                 )
                THEN dqrs.record_id
            END) AS BIGINT
        ) AS valid_records,
        CAST(
            COUNT(DISTINCT dqrs.record_id)
            - COUNT(DISTINCT CASE
                WHEN dqrs.record_id IS NOT NULL
                 AND dqrs.record_id NOT IN (
                    SELECT record_id
                    FROM etl_data_quality_results_silver
                    WHERE CAST(detected_at AS DATE) = CAST(dqrs.detected_at AS DATE)
                      AND source_table = dqrs.source_table
                      AND check_status = 'FAIL'
                 )
                THEN dqrs.record_id
            END) AS BIGINT
        ) AS invalid_records,
        CAST(
            (
                COUNT(DISTINCT CASE
                    WHEN dqrs.record_id IS NOT NULL
                     AND dqrs.record_id NOT IN (
                        SELECT record_id
                        FROM etl_data_quality_results_silver
                        WHERE CAST(detected_at AS DATE) = CAST(dqrs.detected_at AS DATE)
                          AND source_table = dqrs.source_table
                          AND check_status = 'FAIL'
                     )
                    THEN dqrs.record_id
                END) * 100.0
            ) / NULLIF(COUNT(DISTINCT dqrs.record_id), 0)
            AS DECIMAL(5,2)
        ) AS validation_accuracy_pct
    FROM etl_data_quality_results_silver dqrs
    GROUP BY
        CAST(dqrs.detected_at AS DATE),
        CAST(dqrs.source_table AS STRING)
    """
)

(
    gold_etl_data_quality_results_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_data_quality_results.csv")
)

job.commit()