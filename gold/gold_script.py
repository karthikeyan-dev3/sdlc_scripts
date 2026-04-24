```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# SOURCE TABLES (S3 -> DataFrames)
# NOTE: Per requirement, path is constructed as: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)

ingestion_metadata_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestion_metadata_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# TEMP VIEWS
# -----------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")
ingestion_metadata_silver_df.createOrReplaceTempView("ingestion_metadata_silver")

# ===================================================================================
# TARGET: gold.gold_customer_orders
# Mapping: silver.customer_orders_silver cos
# ===================================================================================
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)              AS order_id,
        DATE(cos.order_date)                      AS order_date,
        CAST(cos.customer_id AS STRING)           AS customer_id,
        CAST(cos.order_status AS STRING)          AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
        CAST(cos.currency_code AS STRING)         AS currency_code,
        CAST(cos.source_system AS STRING)         AS source_system,
        DATE(cos.ingestion_date)                  AS ingestion_date
    FROM customer_orders_silver cos
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET: gold.gold_customer_order_items
# Mapping: silver.customer_order_items_silver cois
# ===================================================================================
gold_customer_order_items_df = spark.sql(
    """
    SELECT
        CAST(cois.order_id AS STRING)                  AS order_id,
        CAST(cois.order_item_id AS STRING)             AS order_item_id,
        CAST(cois.product_id AS STRING)                AS product_id,
        CAST(cois.quantity AS INT)                     AS quantity,
        CAST(cois.unit_price_amount AS DECIMAL(38, 10)) AS unit_price_amount,
        CAST(cois.line_total_amount AS DECIMAL(38, 10)) AS line_total_amount,
        CAST(cois.currency_code AS STRING)             AS currency_code,
        CAST(cois.source_system AS STRING)             AS source_system,
        DATE(cois.ingestion_date)                      AS ingestion_date
    FROM customer_order_items_silver cois
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# TARGET: gold.gold_ingestion_metadata
# Mapping: silver.ingestion_metadata_silver ims
# ===================================================================================
gold_ingestion_metadata_df = spark.sql(
    """
    SELECT
        CAST(ims.dataset_name AS STRING)         AS dataset_name,
        CAST(ims.source_location AS STRING)      AS source_location,
        CAST(ims.file_name AS STRING)            AS file_name,
        CAST(ims.file_format AS STRING)          AS file_format,
        DATE(ims.ingestion_date)                 AS ingestion_date,
        CAST(ims.load_timestamp AS TIMESTAMP)    AS load_timestamp,
        CAST(ims.record_count AS INT)            AS record_count,
        CAST(ims.valid_record_count AS INT)      AS valid_record_count,
        CAST(ims.invalid_record_count AS INT)    AS invalid_record_count,
        CAST(ims.validation_status AS STRING)    AS validation_status
    FROM ingestion_metadata_silver ims
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_ingestion_metadata_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_ingestion_metadata.csv")
)

job.commit()
```