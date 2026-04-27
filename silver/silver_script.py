import sys
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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source Reads + Temp Views
# ------------------------------------------------------------------------------------
customer_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)
customer_orders_src_df.createOrReplaceTempView("customer_orders")

customer_orders_dq_summary_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_dq_summary.{FILE_FORMAT}/")
)
customer_orders_dq_summary_src_df.createOrReplaceTempView("customer_orders_dq_summary")

# ------------------------------------------------------------------------------------
# Target: silver.customer_orders
# ------------------------------------------------------------------------------------
customer_orders_out_df = spark.sql(
    """
    WITH base AS (
        SELECT
            co.transaction_id AS order_id,
            CAST(co.transaction_time AS DATE) AS order_date,
            co.store_id AS customer_id,
            co.product_id AS product_id,
            CAST(co.quantity AS INT) AS quantity,
            CASE
                WHEN co.quantity IS NOT NULL AND CAST(co.quantity AS DOUBLE) <> 0
                    THEN CAST(co.sale_amount AS DECIMAL(38, 10)) / CAST(co.quantity AS DECIMAL(38, 10))
                ELSE NULL
            END AS unit_price,
            CAST(co.sale_amount AS DECIMAL(38, 10)) AS order_amount,
            'COMPLETED' AS order_status,
            CAST(co.transaction_time AS DATE) AS source_file_date,
            CURRENT_TIMESTAMP AS load_timestamp,
            co.transaction_time AS transaction_time_ts
        FROM customer_orders co
    ),
    dedup AS (
        SELECT
            order_id,
            order_date,
            customer_id,
            product_id,
            quantity,
            unit_price,
            order_amount,
            order_status,
            source_file_date,
            load_timestamp,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_time_ts DESC) AS rn
        FROM base
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        unit_price,
        order_amount,
        order_status,
        source_file_date,
        load_timestamp
    FROM dedup
    WHERE rn = 1
    """
)

(
    customer_orders_out_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders.csv")
)

# ------------------------------------------------------------------------------------
# Target: silver.customer_orders_dq_summary
# ------------------------------------------------------------------------------------
customer_orders_dq_summary_out_df = spark.sql(
    """
    SELECT
        CAST(codq.transaction_time AS DATE) AS process_date,
        COUNT(*) AS total_records,
        SUM(
            CASE
                WHEN codq.transaction_id IS NOT NULL
                 AND codq.store_id IS NOT NULL
                 AND codq.product_id IS NOT NULL
                 AND codq.quantity IS NOT NULL
                 AND CAST(codq.quantity AS INT) > 0
                 AND codq.sale_amount IS NOT NULL
                 AND CAST(codq.sale_amount AS DECIMAL(38, 10)) >= 0
                 AND codq.transaction_time IS NOT NULL
                THEN 1 ELSE 0
            END
        ) AS valid_records,
        COUNT(*) - SUM(
            CASE
                WHEN codq.transaction_id IS NOT NULL
                 AND codq.store_id IS NOT NULL
                 AND codq.product_id IS NOT NULL
                 AND codq.quantity IS NOT NULL
                 AND CAST(codq.quantity AS INT) > 0
                 AND codq.sale_amount IS NOT NULL
                 AND CAST(codq.sale_amount AS DECIMAL(38, 10)) >= 0
                 AND codq.transaction_time IS NOT NULL
                THEN 1 ELSE 0
            END
        ) AS invalid_records,
        CASE
            WHEN COUNT(*) = 0 THEN CAST(0 AS DECIMAL(18, 4))
            ELSE
                (
                    CAST(
                        SUM(
                            CASE
                                WHEN codq.transaction_id IS NOT NULL
                                 AND codq.store_id IS NOT NULL
                                 AND codq.product_id IS NOT NULL
                                 AND codq.quantity IS NOT NULL
                                 AND CAST(codq.quantity AS INT) > 0
                                 AND codq.sale_amount IS NOT NULL
                                 AND CAST(codq.sale_amount AS DECIMAL(38, 10)) >= 0
                                 AND codq.transaction_time IS NOT NULL
                                THEN 1 ELSE 0
                            END
                        ) AS DECIMAL(18, 4)
                    )
                    / CAST(COUNT(*) AS DECIMAL(18, 4))
                ) * CAST(100 AS DECIMAL(18, 4))
        END AS data_quality_score_pct,
        CAST((unix_timestamp(CURRENT_TIMESTAMP) - unix_timestamp(MAX(codq.transaction_time))) / 60 AS INT) AS ingestion_latency_minutes,
        CURRENT_TIMESTAMP AS loaded_to_redshift_timestamp
    FROM customer_orders_dq_summary codq
    GROUP BY CAST(codq.transaction_time AS DATE)
    """
)

(
    customer_orders_dq_summary_out_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_dq_summary.csv")
)

job.commit()
