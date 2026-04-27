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

# ----------------------------
# 1) Read source tables (Bronze)
# ----------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

customer_order_line_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_items_bronze.{FILE_FORMAT}/")
)

etl_batch_quality_metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_batch_quality_metrics_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")
customer_order_line_items_bronze_df.createOrReplaceTempView("customer_order_line_items_bronze")
etl_batch_quality_metrics_bronze_df.createOrReplaceTempView("etl_batch_quality_metrics_bronze")

# ============================================================
# TABLE: silver.customer_orders_silver
# ============================================================
customer_orders_silver_df = spark.sql(
    """
    WITH dedup AS (
      SELECT
        cob.*,
        ROW_NUMBER() OVER (
          PARTITION BY cob.transaction_id
          ORDER BY cob.transaction_time DESC
        ) AS rn
      FROM customer_orders_bronze cob
    )
    SELECT
      CAST(cob.transaction_id AS STRING)                        AS order_id,
      CAST(cob.transaction_time AS TIMESTAMP)                  AS order_timestamp,
      CAST(cob.transaction_time AS DATE)                       AS order_date,
      CAST(cob.sale_amount AS DECIMAL(18,2))                   AS order_total_amount,
      CAST(NULL AS STRING)                                     AS customer_id,
      CAST(NULL AS STRING)                                     AS order_status,
      CAST(NULL AS STRING)                                     AS currency_code,
      CAST(NULL AS STRING)                                     AS payment_method,
      CAST(NULL AS STRING)                                     AS shipping_address_line1,
      CAST(NULL AS STRING)                                     AS shipping_address_line2,
      CAST(NULL AS STRING)                                     AS shipping_city,
      CAST(NULL AS STRING)                                     AS shipping_state,
      CAST(NULL AS STRING)                                     AS shipping_postal_code,
      CAST(NULL AS STRING)                                     AS shipping_country,
      CURRENT_DATE()                                           AS ingestion_date
    FROM dedup cob
    WHERE cob.rn = 1
    """
)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ============================================================
# TABLE: silver.customer_order_line_items_silver
# ============================================================
customer_order_line_items_silver_df = spark.sql(
    """
    WITH orders_dedup AS (
      SELECT
        cob.*,
        ROW_NUMBER() OVER (
          PARTITION BY cob.transaction_id
          ORDER BY cob.transaction_time DESC
        ) AS rn
      FROM customer_orders_bronze cob
    ),
    base AS (
      SELECT
        colib.*,
        cob.transaction_time AS order_transaction_time,
        cob.sale_amount      AS order_sale_amount
      FROM customer_order_line_items_bronze colib
      LEFT JOIN orders_dedup cob
        ON colib.transaction_id = cob.transaction_id
       AND cob.rn = 1
    ),
    line_dedup AS (
      SELECT
        b.*,
        ROW_NUMBER() OVER (
          PARTITION BY b.transaction_id, b.product_id
          ORDER BY b.order_transaction_time DESC
        ) AS rn
      FROM base b
    )
    SELECT
      CAST(MD5(CONCAT(CAST(ld.transaction_id AS STRING), '|', CAST(ld.product_id AS STRING))) AS STRING) AS line_item_id,
      CAST(ld.transaction_id AS STRING)                                                                AS order_id,
      CAST(ld.product_id AS STRING)                                                                    AS product_id,
      CAST(ld.quantity AS INT)                                                                         AS quantity,
      CAST(
        CAST(ld.quantity AS DECIMAL(18,2))
        *
        (
          CAST(ld.order_sale_amount AS DECIMAL(18,2))
          /
          NULLIF(COUNT(*) OVER (PARTITION BY ld.transaction_id), 0)
        )
        AS DECIMAL(18,2)
      )                                                                                                AS line_total_amount,
      CAST(NULL AS STRING)                                                                             AS currency_code,
      CAST(NULL AS STRING)                                                                             AS product_name,
      CAST(NULL AS STRING)                                                                             AS product_category,
      CAST(NULL AS DECIMAL(18,2))                                                                      AS unit_price,
      CURRENT_DATE()                                                                                   AS ingestion_date
    FROM line_dedup ld
    WHERE ld.rn = 1
    """
)

(
    customer_order_line_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/customer_order_line_items_silver.csv")
)

# ============================================================
# TABLE: silver.etl_batch_quality_metrics_silver
# ============================================================
etl_batch_quality_metrics_silver_df = spark.sql(
    """
    WITH agg AS (
      SELECT
        CAST(ebqmb.load_timestamp AS DATE)                         AS batch_date,
        SUM(ebqmb.source_row_count)                                AS records_received,
        SUM(ebqmb.source_row_count) - COALESCE(SUM(ebqmb.rejected_row_count), 0) AS records_loaded,
        COALESCE(SUM(ebqmb.rejected_row_count), 0)                 AS records_rejected,
        MIN(ebqmb.load_timestamp)                                  AS ingestion_start_ts,
        MAX(ebqmb.load_timestamp)                                  AS ingestion_end_ts
      FROM etl_batch_quality_metrics_bronze ebqmb
      GROUP BY CAST(ebqmb.load_timestamp AS DATE)
    )
    SELECT
      a.batch_date                                                  AS batch_date,
      CAST('sales_transactions_raw' AS STRING)                      AS source_system,
      CAST(a.records_received AS BIGINT)                            AS records_received,
      CAST(a.records_loaded AS BIGINT)                              AS records_loaded,
      CAST(a.records_rejected AS BIGINT)                            AS records_rejected,
      CASE
        WHEN a.records_received = 0 THEN CAST(NULL AS DECIMAL(18,6))
        ELSE CAST(a.records_loaded AS DECIMAL(18,6)) / CAST(a.records_received AS DECIMAL(18,6))
      END                                                           AS data_quality_score,
      CAST(a.ingestion_start_ts AS TIMESTAMP)                       AS ingestion_start_ts,
      CAST(a.ingestion_end_ts AS TIMESTAMP)                         AS ingestion_end_ts,
      CAST((UNIX_TIMESTAMP(a.ingestion_end_ts) - UNIX_TIMESTAMP(a.ingestion_start_ts)) / 60.0 AS DECIMAL(18,2))
                                                                    AS ingestion_duration_minutes,
      CURRENT_DATE()                                                AS ingestion_date
    FROM agg a
    """
)

(
    etl_batch_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/etl_batch_quality_metrics_silver.csv")
)

job.commit()