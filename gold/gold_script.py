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
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Source Reads (S3) + Temp Views
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

order_validation_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_validation_daily_silver.{FILE_FORMAT}/")
)
order_validation_daily_silver_df.createOrReplaceTempView("order_validation_daily_silver")

# ===================================================================================
# TARGET TABLE: gold.gold_customer_orders
# Source: silver.customer_orders_silver cos
# ===================================================================================
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(cos.order_id AS STRING)                AS order_id,
  CAST(cos.order_date AS DATE)                AS order_date,
  CAST(cos.order_timestamp AS TIMESTAMP)      AS order_timestamp,
  CAST(cos.product_id AS STRING)              AS product_id,
  CAST(cos.quantity AS INT)                   AS quantity,
  CAST(cos.unit_price AS DECIMAL(38, 18))     AS unit_price,
  CAST(cos.order_total_amount AS DECIMAL(38, 18)) AS order_total_amount
FROM customer_orders_silver cos
""")

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE: gold.gold_order_validation_daily
# Source: silver.order_validation_daily_silver ovds
# ===================================================================================
gold_order_validation_daily_df = spark.sql("""
SELECT
  CAST(ovds.ingest_date AS DATE)                  AS ingest_date,
  CAST(ovds.records_received AS INT)              AS records_received,
  CAST(ovds.schema_validation_passed AS INT)      AS schema_validation_passed,
  CAST(ovds.data_quality_passed AS INT)           AS data_quality_passed,
  CAST(ovds.records_rejected AS INT)              AS records_rejected,
  CAST(ovds.records_loaded AS INT)                AS records_loaded,
  CAST(ovds.validation_pass_rate AS DECIMAL(38, 18)) AS validation_pass_rate,
  CAST(ovds.first_record_timestamp AS TIMESTAMP)  AS first_record_timestamp,
  CAST(ovds.last_record_timestamp AS TIMESTAMP)   AS last_record_timestamp,
  CAST(ovds.load_start_timestamp AS TIMESTAMP)    AS load_start_timestamp,
  CAST(ovds.load_end_timestamp AS TIMESTAMP)      AS load_end_timestamp
FROM order_validation_daily_silver ovds
""")

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_order_validation_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_validation_daily.csv")
)

job.commit()
```