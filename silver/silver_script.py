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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------
# READ SOURCE TABLES FROM S3 (BRONZE)
# -------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)
customers_bronze_df.createOrReplaceTempView("customers_bronze")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

etl_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_runs_bronze.{FILE_FORMAT}/")
)
etl_runs_bronze_df.createOrReplaceTempView("etl_runs_bronze")

data_quality_metrics_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_bronze.{FILE_FORMAT}/")
)
data_quality_metrics_bronze_df.createOrReplaceTempView("data_quality_metrics_bronze")

currencies_reference_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/currencies_reference_bronze.{FILE_FORMAT}/")
)
currencies_reference_bronze_df.createOrReplaceTempView("currencies_reference_bronze")

order_status_reference_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_status_reference_bronze.{FILE_FORMAT}/")
)
order_status_reference_bronze_df.createOrReplaceTempView("order_status_reference_bronze")

source_systems_reference_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/source_systems_reference_bronze.{FILE_FORMAT}/")
)
source_systems_reference_bronze_df.createOrReplaceTempView("source_systems_reference_bronze")

datasets_reference_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/datasets_reference_bronze.{FILE_FORMAT}/")
)
datasets_reference_bronze_df.createOrReplaceTempView("datasets_reference_bronze")

# -------------------------------------
# TARGET: currencies_silver
# -------------------------------------
currencies_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(CAST(crb.currency_code AS STRING))) AS currency_code,
        crb.*
      FROM currencies_reference_bronze crb
    ),
    ranked AS (
      SELECT
        currency_code,
        ROW_NUMBER() OVER (
          PARTITION BY currency_code
          ORDER BY currency_code
        ) AS rn
      FROM base
      WHERE currency_code IS NOT NULL AND TRIM(currency_code) <> ''
    )
    SELECT
      currency_code
    FROM ranked
    WHERE rn = 1
    """
)

currencies_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/currencies_silver.csv"
)
currencies_silver_df.createOrReplaceTempView("currencies_silver")

# -------------------------------------
# TARGET: order_statuses_silver
# -------------------------------------
order_statuses_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(CAST(osrb.order_status_code AS STRING))) AS order_status_code
      FROM order_status_reference_bronze osrb
    ),
    ranked AS (
      SELECT
        order_status_code,
        ROW_NUMBER() OVER (
          PARTITION BY order_status_code
          ORDER BY order_status_code
        ) AS rn
      FROM base
      WHERE order_status_code IS NOT NULL AND TRIM(order_status_code) <> ''
    )
    SELECT
      order_status_code
    FROM ranked
    WHERE rn = 1
    """
)

order_statuses_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/order_statuses_silver.csv"
)
order_statuses_silver_df.createOrReplaceTempView("order_statuses_silver")

# -------------------------------------
# TARGET: source_systems_silver
# -------------------------------------
source_systems_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(CAST(ssrb.source_system_code AS STRING))) AS source_system_code,
        TRIM(CAST(ssrb.source_system_name AS STRING)) AS source_system_name
      FROM source_systems_reference_bronze ssrb
    ),
    ranked AS (
      SELECT
        source_system_code,
        source_system_name,
        ROW_NUMBER() OVER (
          PARTITION BY source_system_code
          ORDER BY source_system_name
        ) AS rn
      FROM base
      WHERE source_system_code IS NOT NULL AND TRIM(source_system_code) <> ''
    )
    SELECT
      source_system_code,
      source_system_name
    FROM ranked
    WHERE rn = 1
    """
)

source_systems_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/source_systems_silver.csv"
)
source_systems_silver_df.createOrReplaceTempView("source_systems_silver")

# -------------------------------------
# TARGET: datasets_silver
# -------------------------------------
datasets_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(CAST(dsrb.dataset_name AS STRING)) AS dataset_name
      FROM datasets_reference_bronze dsrb
    ),
    ranked AS (
      SELECT
        dataset_name,
        ROW_NUMBER() OVER (
          PARTITION BY dataset_name
          ORDER BY dataset_name
        ) AS rn
      FROM base
      WHERE dataset_name IS NOT NULL AND TRIM(dataset_name) <> ''
    )
    SELECT
      dataset_name
    FROM ranked
    WHERE rn = 1
    """
)

datasets_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/datasets_silver.csv"
)
datasets_silver_df.createOrReplaceTempView("datasets_silver")

# -------------------------------------
# TARGET: customer_orders_silver
# -------------------------------------
customer_orders_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(ob.order_id AS STRING) AS order_id,
        ob.order_timestamp AS order_timestamp,
        CAST(ob.order_timestamp AS DATE) AS order_date,
        ob.order_total_amount AS order_total_amount,
        cur.currency_code AS currency_code,
        os.order_status_code AS order_status,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(ob.order_id AS STRING)
          ORDER BY ob.order_timestamp DESC
        ) AS rn
      FROM orders_bronze ob
      LEFT JOIN customers_bronze cb
        ON ob.customer_id = cb.customer_id
      LEFT JOIN currencies_silver cur
        ON UPPER(TRIM(CAST(ob.currency_code AS STRING))) = cur.currency_code
      LEFT JOIN order_statuses_silver os
        ON UPPER(TRIM(CAST(ob.order_status AS STRING))) = os.order_status_code
      WHERE CAST(ob.order_id AS STRING) IS NOT NULL
        AND TRIM(CAST(ob.order_id AS STRING)) <> ''
    )
    SELECT
      order_id,
      order_timestamp,
      order_date,
      order_total_amount,
      currency_code,
      order_status,
      CURRENT_DATE() AS ingestion_date
    FROM joined
    WHERE rn = 1
    """
)

customer_orders_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_orders_silver.csv"
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# -------------------------------------
# TARGET: customer_order_items_silver
# -------------------------------------
customer_order_items_silver_df = spark.sql(
    """
    WITH staged AS (
      SELECT
        md5(
          concat(
            CAST(oib.order_id AS STRING),
            '|',
            COALESCE(CAST(oib.product_id AS STRING), ''),
            '|',
            COALESCE(CAST(oib.transaction_time AS STRING), ''),
            '|',
            COALESCE(CAST(oib.store_id AS STRING), '')
          )
        ) AS order_item_id,
        cos.order_id AS order_id,
        CAST(oib.product_id AS STRING) AS product_id,
        CASE WHEN oib.quantity = 0 THEN NULL ELSE oib.quantity END AS quantity,
        CASE
          WHEN oib.quantity IS NOT NULL AND oib.quantity <> 0 THEN oib.line_amount / oib.quantity
          ELSE NULL
        END AS unit_price_amount,
        oib.line_amount AS line_total_amount,
        oib.transaction_time AS transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY md5(
            concat(
              CAST(oib.order_id AS STRING),
              '|',
              COALESCE(CAST(oib.product_id AS STRING), ''),
              '|',
              COALESCE(CAST(oib.transaction_time AS STRING), ''),
              '|',
              COALESCE(CAST(oib.store_id AS STRING), '')
            )
          )
          ORDER BY oib.transaction_time DESC
        ) AS rn
      FROM order_items_bronze oib
      LEFT JOIN products_bronze pb
        ON oib.product_id = pb.product_id
      INNER JOIN customer_orders_silver cos
        ON oib.order_id = cos.order_id
      WHERE oib.order_id IS NOT NULL
        AND TRIM(CAST(oib.order_id AS STRING)) <> ''
    )
    SELECT
      order_item_id,
      order_id,
      product_id,
      quantity,
      unit_price_amount,
      line_total_amount
    FROM staged
    WHERE rn = 1
    """
)

customer_order_items_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/customer_order_items_silver.csv"
)
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

# -------------------------------------
# TARGET: etl_data_quality_daily_silver
# -------------------------------------
etl_data_quality_daily_silver_df = spark.sql(
    """
    WITH aggregated AS (
      SELECT
        CAST(erb.run_timestamp AS DATE) AS run_date,
        sss.source_system_name AS source_system,
        dss.dataset_name AS dataset_name,
        SUM(dqmb.records_ingested) AS records_ingested,
        SUM(dqmb.records_valid) AS records_valid,
        SUM(dqmb.records_rejected) AS records_rejected,
        CASE
          WHEN SUM(dqmb.records_ingested) > 0
            THEN (CAST(SUM(dqmb.records_valid) AS DECIMAL(18,4)) / CAST(SUM(dqmb.records_ingested) AS DECIMAL(18,4))) * 100
          ELSE NULL
        END AS validation_accuracy_pct
      FROM etl_runs_bronze erb
      INNER JOIN data_quality_metrics_bronze dqmb
        ON erb.run_id = dqmb.run_id
      INNER JOIN source_systems_silver sss
        ON erb.source_system_code = sss.source_system_code
      INNER JOIN datasets_silver dss
        ON dqmb.dataset_name = dss.dataset_name
      GROUP BY
        CAST(erb.run_timestamp AS DATE),
        sss.source_system_name,
        dss.dataset_name
    )
    SELECT
      run_date,
      source_system,
      dataset_name,
      CASE WHEN records_ingested < 0 THEN 0 ELSE records_ingested END AS records_ingested,
      CASE WHEN records_valid < 0 THEN 0 ELSE records_valid END AS records_valid,
      CASE WHEN records_rejected < 0 THEN 0 ELSE records_rejected END AS records_rejected,
      CASE
        WHEN validation_accuracy_pct IS NULL THEN NULL
        WHEN validation_accuracy_pct < 0 THEN CAST(0 AS DECIMAL(18,4))
        WHEN validation_accuracy_pct > 100 THEN CAST(100 AS DECIMAL(18,4))
        ELSE CAST(validation_accuracy_pct AS DECIMAL(18,4))
      END AS validation_accuracy_pct
    FROM aggregated
    """
)

etl_data_quality_daily_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/etl_data_quality_daily_silver.csv"
)
etl_data_quality_daily_silver_df.createOrReplaceTempView("etl_data_quality_daily_silver")

job.commit()