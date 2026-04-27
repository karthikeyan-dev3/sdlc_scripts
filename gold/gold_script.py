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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# -----------------------------------------------------------------------------------
# Source: silver.customer_orders_silver (cos)
# -----------------------------------------------------------------------------------
cos_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
cos_df.createOrReplaceTempView("customer_orders_silver")

gcod_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(cos.order_date AS DATE)                         AS order_date,
            CAST(cos.order_id AS STRING)                         AS order_id,
            CAST(cos.customer_id AS STRING)                      AS customer_id,
            CAST(cos.order_status AS STRING)                     AS order_status,
            CAST(cos.order_total_amount AS DECIMAL(38,10))       AS order_total_amount,
            CAST(cos.currency_code AS STRING)                    AS currency_code,
            CAST(cos.item_count AS INT)                          AS item_count,
            CAST(cos.ingested_at AS TIMESTAMP)                   AS ingested_at,
            CAST(cos.source_file_name AS STRING)                 AS source_file_name
        FROM customer_orders_silver cos
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY ingested_at DESC, source_file_name DESC
            ) AS rn
        FROM base
    )
    SELECT
        order_date,
        order_id,
        customer_id,
        order_status,
        order_total_amount,
        currency_code,
        item_count,
        ingested_at,
        source_file_name
    FROM dedup
    WHERE rn = 1
    """
)

(
    gcod_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders_daily.csv")
)

# -----------------------------------------------------------------------------------
# Source: silver.customer_orders_daily_dq_summary_silver (codqs)
# -----------------------------------------------------------------------------------
codqs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_daily_dq_summary_silver.{FILE_FORMAT}/")
)
codqs_df.createOrReplaceTempView("customer_orders_daily_dq_summary_silver")

gcodqs_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(codqs.order_date AS DATE)                   AS order_date,
            CAST(codqs.total_records AS INT)                 AS total_records,
            CAST(codqs.valid_records AS INT)                 AS valid_records,
            CAST(codqs.invalid_records AS INT)               AS invalid_records,
            CAST(codqs.data_quality_rate AS DECIMAL(38,10))  AS data_quality_rate,
            CAST(codqs.processed_at AS TIMESTAMP)            AS processed_at
        FROM customer_orders_daily_dq_summary_silver codqs
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_date
                ORDER BY processed_at DESC
            ) AS rn
        FROM base
    )
    SELECT
        order_date,
        total_records,
        valid_records,
        invalid_records,
        data_quality_rate,
        processed_at
    FROM dedup
    WHERE rn = 1
    """
)

(
    gcodqs_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders_daily_dq_summary.csv")
)

# -----------------------------------------------------------------------------------
# Source: silver.customer_orders_daily_sla_silver (coslas)
# -----------------------------------------------------------------------------------
coslas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_daily_sla_silver.{FILE_FORMAT}/")
)
coslas_df.createOrReplaceTempView("customer_orders_daily_sla_silver")

gcodsla_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(coslas.order_date AS DATE)                 AS order_date,
            CAST(coslas.expected_complete_by AS TIMESTAMP)  AS expected_complete_by,
            CAST(coslas.actual_complete_at AS TIMESTAMP)    AS actual_complete_at,
            CAST(coslas.sla_met_flag AS BOOLEAN)            AS sla_met_flag
        FROM customer_orders_daily_sla_silver coslas
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_date
                ORDER BY actual_complete_at DESC, expected_complete_by DESC
            ) AS rn
        FROM base
    )
    SELECT
        order_date,
        expected_complete_by,
        actual_complete_at,
        sla_met_flag
    FROM dedup
    WHERE rn = 1
    """
)

(
    gcodsla_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_customer_orders_daily_sla.csv")
)

job.commit()