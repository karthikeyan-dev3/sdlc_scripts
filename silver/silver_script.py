import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ------------------------------------------------------------------------------------
# 1) READ SOURCE TABLES (S3) + CREATE TEMP VIEWS
# ------------------------------------------------------------------------------------

customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

etl_data_quality_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_results_bronze.{FILE_FORMAT}/")
)
etl_data_quality_results_bronze_df.createOrReplaceTempView("etl_data_quality_results_bronze")

# ------------------------------------------------------------------------------------
# TARGET: silver.customer_orders_silver
# Sources:
#   bronze.customer_orders_bronze cob
#   LEFT JOIN bronze.etl_data_quality_results_bronze dqrb
# Dedup: latest by transaction_time (and detected_at as tiebreaker) per transaction_id
# ------------------------------------------------------------------------------------

customer_orders_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        cob.*,
        dqrb.detected_at AS dq_detected_at,
        dqrb.source_table AS dq_source_table
      FROM customer_orders_bronze cob
      LEFT JOIN etl_data_quality_results_bronze dqrb
        ON dqrb.record_id = cob.transaction_id
       AND dqrb.source_table = 'sales_transactions_raw'
    ),
    ranked AS (
      SELECT
        CAST(transaction_id AS STRING) AS order_id,
        CAST(transaction_time AS DATE) AS order_date,
        CAST(sale_amount AS DECIMAL(18,2)) AS order_total_amount,
        CAST(GREATEST(dq_detected_at, transaction_time) AS DATE) AS ingestion_date,
        CAST(dq_source_table AS STRING) AS source_system,

        CAST(NULL AS STRING) AS customer_id,
        CAST(NULL AS STRING) AS order_status,
        CAST(NULL AS STRING) AS currency_code,
        CAST(NULL AS STRING) AS payment_method,
        CAST(NULL AS STRING) AS shipping_country,
        CAST(NULL AS STRING) AS shipping_state,
        CAST(NULL AS STRING) AS shipping_city,

        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC, dq_detected_at DESC
        ) AS rn
      FROM base
    )
    SELECT
      order_id,
      order_date,
      order_total_amount,
      customer_id,
      order_status,
      currency_code,
      payment_method,
      shipping_country,
      shipping_state,
      shipping_city,
      source_system,
      ingestion_date
    FROM ranked
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

# ------------------------------------------------------------------------------------
# TARGET: silver.customer_order_items_silver
# Sources:
#   bronze.customer_order_items_bronze coib
#   INNER JOIN bronze.customer_orders_bronze cob
# Dedup/Aggregate: sum quantity per (transaction_id, product_id); keep latest event per key
# ------------------------------------------------------------------------------------

customer_order_items_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        coib.*,
        cob.sale_amount,
        cob.transaction_time
      FROM customer_order_items_bronze coib
      INNER JOIN customer_orders_bronze cob
        ON cob.transaction_id = coib.transaction_id
    ),
    cleaned AS (
      SELECT
        CAST(transaction_id AS STRING) AS order_id,
        TRIM(CAST(product_id AS STRING)) AS product_id,
        COALESCE(CAST(quantity AS INT), 0) AS quantity,
        CAST(sale_amount AS DECIMAL(18,2)) AS sale_amount,
        transaction_time
      FROM joined
      WHERE COALESCE(CAST(quantity AS INT), 0) >= 0
    ),
    aggregated AS (
      SELECT
        order_id,
        product_id,
        SUM(quantity) AS quantity,
        MAX(sale_amount) AS sale_amount,
        MAX(transaction_time) AS transaction_time
      FROM cleaned
      GROUP BY order_id, product_id
    ),
    priced AS (
      SELECT
        order_id,
        product_id,
        quantity,
        CAST(
          sale_amount / NULLIF(SUM(quantity) OVER (PARTITION BY order_id), 0)
          AS DECIMAL(18,2)
        ) AS unit_price_amount,
        CAST(
          (
            sale_amount / NULLIF(SUM(quantity) OVER (PARTITION BY order_id), 0)
          ) * quantity
          AS DECIMAL(18,2)
        ) AS line_total_amount,
        ROW_NUMBER() OVER (
          PARTITION BY order_id, product_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM aggregated
    )
    SELECT
      order_id,
      CAST(xxhash64(order_id, product_id) AS STRING) AS order_item_id,
      product_id,
      quantity,
      unit_price_amount,
      line_total_amount
    FROM priced
    WHERE rn = 1
    """
)

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: silver.etl_data_quality_results_silver
# Source:
#   bronze.etl_data_quality_results_bronze dqrb
# Dedup: one row per (source_table, record_id, check_name, detected_at)
# ------------------------------------------------------------------------------------

etl_data_quality_results_silver_df = spark.sql(
    """
    WITH standardized AS (
      SELECT
        dqrb.detected_at AS detected_at,
        CAST(TRIM(dqrb.source_table) AS STRING) AS source_table,
        CAST(TRIM(dqrb.record_id) AS STRING) AS record_id,
        CAST(TRIM(dqrb.check_name) AS STRING) AS check_name,
        CAST(UPPER(TRIM(dqrb.check_status)) AS STRING) AS check_status,
        CAST(dqrb.check_message AS STRING) AS check_message
      FROM etl_data_quality_results_bronze dqrb
    ),
    ranked AS (
      SELECT
        detected_at,
        source_table,
        record_id,
        check_name,
        check_status,
        check_message,
        ROW_NUMBER() OVER (
          PARTITION BY source_table, record_id, check_name, detected_at
          ORDER BY detected_at DESC
        ) AS rn
      FROM standardized
    )
    SELECT
      detected_at,
      source_table,
      record_id,
      check_name,
      check_status,
      check_message
    FROM ranked
    WHERE rn = 1
    """
)

(
    etl_data_quality_results_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/etl_data_quality_results_silver.csv")
)