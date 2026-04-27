import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# --------------------------------------------------------------------------------------
# SOURCE: bronze.customer_orders_bronze (cob)
# TARGET: silver.customer_orders_silver (cos)
# --------------------------------------------------------------------------------------
cob_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
cob_df.createOrReplaceTempView("cob")

customer_orders_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            cob.transaction_id                                                AS order_id,
            CAST(cob.transaction_time AS DATE)                                AS order_date,
            cob.sale_amount                                                   AS order_total_amount,
            cob.quantity                                                      AS item_count,
            CAST(NULL AS STRING)                                              AS order_status,
            CAST(NULL AS STRING)                                              AS customer_id,
            CAST(NULL AS STRING)                                              AS currency_code,
            cob.ingested_at                                                   AS ingested_at,
            cob.source_file_name                                              AS source_file_name,
            ROW_NUMBER() OVER (
                PARTITION BY cob.transaction_id
                ORDER BY cob.transaction_time DESC, cob.ingested_at DESC
            )                                                                 AS rn
        FROM cob
    )
    SELECT
        order_id,
        order_date,
        order_total_amount,
        item_count,
        order_status,
        customer_id,
        currency_code,
        ingested_at,
        source_file_name
    FROM base
    WHERE rn = 1
    """
)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# --------------------------------------------------------------------------------------
# SOURCE: silver.customer_orders_silver (cos)
# TARGET: silver.customer_orders_daily_dq_summary_silver (codqs)
# --------------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("cos")

customer_orders_daily_dq_summary_silver_df = spark.sql(
    """
    SELECT
        cos.order_date                                                                 AS order_date,
        COUNT(cos.order_id)                                                            AS total_records,
        SUM(
            CASE
                WHEN cos.order_id IS NOT NULL
                 AND cos.order_date IS NOT NULL
                 AND cos.order_total_amount IS NOT NULL
                 AND cos.order_total_amount >= 0
                 AND cos.item_count IS NOT NULL
                 AND cos.item_count >= 0
                THEN 1 ELSE 0
            END
        )                                                                              AS valid_records,
        SUM(
            CASE
                WHEN NOT (
                    cos.order_id IS NOT NULL
                    AND cos.order_date IS NOT NULL
                    AND cos.order_total_amount IS NOT NULL
                    AND cos.order_total_amount >= 0
                    AND cos.item_count IS NOT NULL
                    AND cos.item_count >= 0
                )
                THEN 1 ELSE 0
            END
        )                                                                              AS invalid_records,
        CAST(
            (
                SUM(
                    CASE
                        WHEN cos.order_id IS NOT NULL
                         AND cos.order_date IS NOT NULL
                         AND cos.order_total_amount IS NOT NULL
                         AND cos.order_total_amount >= 0
                         AND cos.item_count IS NOT NULL
                         AND cos.item_count >= 0
                        THEN 1 ELSE 0
                    END
                ) / NULLIF(COUNT(cos.order_id), 0)
            ) AS DECIMAL(18,6)
        )                                                                              AS data_quality_rate,
        current_timestamp()                                                            AS processed_at
    FROM cos
    GROUP BY cos.order_date
    """
)

(
    customer_orders_daily_dq_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_daily_dq_summary_silver.csv")
)

# --------------------------------------------------------------------------------------
# SOURCE: silver.customer_orders_silver (cos)
# TARGET: silver.customer_orders_daily_sla_silver (coslas)
# --------------------------------------------------------------------------------------
customer_orders_daily_sla_silver_df = spark.sql(
    """
    SELECT
        cos.order_date                                              AS order_date,
        (CAST(cos.order_date AS TIMESTAMP) + INTERVAL 24 HOURS)      AS expected_complete_by,
        MAX(cos.ingested_at)                                         AS actual_complete_at,
        CASE
            WHEN MAX(cos.ingested_at) <= (CAST(cos.order_date AS TIMESTAMP) + INTERVAL 24 HOURS)
            THEN 1 ELSE 0
        END                                                         AS sla_met_flag
    FROM cos
    GROUP BY cos.order_date
    """
)

(
    customer_orders_daily_sla_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_daily_sla_silver.csv")
)

job.commit()
