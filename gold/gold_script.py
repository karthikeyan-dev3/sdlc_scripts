```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# CSV read options (adjust if your silver files differ)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    NOTE: Path format MUST be: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_order_items.{FILE_FORMAT}/")
)

order_data_quality_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/order_data_quality_summary.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")
order_data_quality_summary_silver_df.createOrReplaceTempView("order_data_quality_summary_silver")

# ===================================================================================
# TARGET TABLE: gold_customer_orders
# mapping_details:
# silver.customer_orders sco LEFT JOIN silver.customer_order_items scoi ON sco.order_id = scoi.order_id
#
# Transformations (EXACT from UDT):
# - order_total_amount = COALESCE(SUM(scoi.line_total_amount), sco.order_total_amount) GROUP BY sco.order_id, sco.order_total_amount
# - item_count = COUNT(scoi.order_id) GROUP BY sco.order_id
# ===================================================================================
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(sco.order_id AS STRING)                      AS order_id,
        CAST(sco.order_date AS DATE)                     AS order_date,
        CAST(sco.customer_id AS STRING)                  AS customer_id,
        CAST(sco.order_status AS STRING)                 AS order_status,
        CAST(sco.currency_code AS STRING)                AS currency_code,

        CAST(
            COALESCE(SUM(CAST(scoi.line_total_amount AS DECIMAL(38,10))),
                     CAST(sco.order_total_amount AS DECIMAL(38,10)))
            AS DECIMAL(38,10)
        ) AS order_total_amount,

        CAST(COUNT(scoi.order_id) AS INT)                AS item_count,
        CAST(sco.source_system AS STRING)                AS source_system,
        CAST(sco.ingestion_date AS DATE)                 AS ingestion_date,
        CAST(sco.load_timestamp AS TIMESTAMP)            AS load_timestamp
    FROM customer_orders_silver sco
    LEFT JOIN customer_order_items_silver scoi
        ON sco.order_id = scoi.order_id
    GROUP BY
        sco.order_id,
        sco.order_date,
        sco.customer_id,
        sco.order_status,
        sco.currency_code,
        sco.order_total_amount,
        sco.source_system,
        sco.ingestion_date,
        sco.load_timestamp
    """
)

# -----------------------------------------------------------------------------------
# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
# -----------------------------------------------------------------------------------
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold_customer_order_items
# mapping_details:
# silver.customer_order_items scoi INNER JOIN silver.customer_orders sco ON scoi.order_id = sco.order_id
# ===================================================================================
gold_customer_order_items_df = spark.sql(
    """
    SELECT
        CAST(scoi.order_id AS STRING)            AS order_id,
        CAST(scoi.order_item_id AS STRING)       AS order_item_id,
        CAST(scoi.product_id AS STRING)          AS product_id,
        CAST(scoi.quantity AS INT)               AS quantity,
        CAST(scoi.unit_price_amount AS DECIMAL(38,10)) AS unit_price_amount,
        CAST(scoi.line_total_amount AS DECIMAL(38,10)) AS line_total_amount
    FROM customer_order_items_silver scoi
    INNER JOIN customer_orders_silver sco
        ON scoi.order_id = sco.order_id
    """
)

# -----------------------------------------------------------------------------------
# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
# -----------------------------------------------------------------------------------
(
    gold_customer_order_items_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# TARGET TABLE: gold_order_data_quality_summary
# mapping_details:
# silver.order_data_quality_summary sodqs
# ===================================================================================
gold_order_data_quality_summary_df = spark.sql(
    """
    SELECT
        CAST(sodqs.ingestion_date AS DATE)              AS ingestion_date,
        CAST(sodqs.total_records AS INT)                AS total_records,
        CAST(sodqs.valid_records AS INT)                AS valid_records,
        CAST(sodqs.invalid_records AS INT)              AS invalid_records,
        CAST(sodqs.data_quality_score_pct AS DECIMAL(38,10)) AS data_quality_score_pct
    FROM order_data_quality_summary_silver sodqs
    """
)

# -----------------------------------------------------------------------------------
# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
# -----------------------------------------------------------------------------------
(
    gold_order_data_quality_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_data_quality_summary.csv")
)

job.commit()
```