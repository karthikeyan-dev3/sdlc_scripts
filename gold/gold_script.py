import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# ----------------------------
# Read source tables from S3
# ----------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

currencies_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/currencies_silver.{FILE_FORMAT}/")
)
currencies_silver_df.createOrReplaceTempView("currencies_silver")

order_statuses_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_statuses_silver.{FILE_FORMAT}/")
)
order_statuses_silver_df.createOrReplaceTempView("order_statuses_silver")

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

etl_data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/etl_data_quality_daily_silver.{FILE_FORMAT}/")
)
etl_data_quality_daily_silver_df.createOrReplaceTempView("etl_data_quality_daily_silver")

source_systems_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/source_systems_silver.{FILE_FORMAT}/")
)
source_systems_silver_df.createOrReplaceTempView("source_systems_silver")

datasets_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/datasets_silver.{FILE_FORMAT}/")
)
datasets_silver_df.createOrReplaceTempView("datasets_silver")

# ---------------------------------------
# Target: gold_customer_orders (gco)
# ---------------------------------------
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)                        AS order_id,
        CAST(cos.order_date AS DATE)                        AS order_date,
        CAST(cos.order_timestamp AS TIMESTAMP)              AS order_timestamp,
        CAST(cos.order_total_amount AS DECIMAL(38, 18))     AS order_total_amount,
        CAST(cur.currency_code AS STRING)                   AS currency_code,
        CAST(os.order_status_code AS STRING)                AS order_status
    FROM customer_orders_silver cos
    LEFT JOIN currencies_silver cur
        ON cos.currency_code = cur.currency_code
    LEFT JOIN order_statuses_silver os
        ON cos.order_status = os.order_status_code
    """
)

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ---------------------------------------
# Target: gold_customer_order_items (gcoi)
# ---------------------------------------
gold_customer_order_items_df = spark.sql(
    """
    SELECT
        CAST(cois.order_item_id AS STRING)                  AS order_item_id,
        CAST(cos.order_id AS STRING)                        AS order_id,
        CAST(cois.product_id AS STRING)                     AS product_id,
        CAST(cois.quantity AS INT)                          AS quantity,
        CAST(cois.unit_price_amount AS DECIMAL(38, 18))     AS unit_price_amount,
        CAST(cois.line_total_amount AS DECIMAL(38, 18))     AS line_total_amount
    FROM customer_order_items_silver cois
    INNER JOIN customer_orders_silver cos
        ON cois.order_id = cos.order_id
    """
)

(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ---------------------------------------
# Target: gold_etl_data_quality_daily (gedqd)
# ---------------------------------------
gold_etl_data_quality_daily_df = spark.sql(
    """
    SELECT
        CAST(dqds.run_date AS DATE)                         AS run_date,
        CAST(sss.source_system_name AS STRING)              AS source_system,
        CAST(dss.dataset_name AS STRING)                    AS dataset_name,
        CAST(dqds.records_ingested AS INT)                  AS records_ingested,
        CAST(dqds.records_valid AS INT)                     AS records_valid,
        CAST(dqds.records_rejected AS INT)                  AS records_rejected,
        CAST(dqds.validation_accuracy_pct AS DECIMAL(38, 18)) AS validation_accuracy_pct
    FROM etl_data_quality_daily_silver dqds
    INNER JOIN source_systems_silver sss
        ON dqds.source_system = sss.source_system_name
    INNER JOIN datasets_silver dss
        ON dqds.dataset_name = dss.dataset_name
    """
)

(
    gold_etl_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_etl_data_quality_daily.csv")
)

job.commit()
