```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Job bootstrap
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

# ===================================================================================
# Target Table: gold.gold_customer_orders
# Source: silver.customer_orders_silver (alias: cos)
# ===================================================================================

# 1) Read source table(s) from S3 (STRICT path format)
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (apply EXACT mappings + required SQL functions)
df_gold_customer_orders = spark.sql(
    """
    SELECT
        CAST(TRIM(cos.order_id) AS STRING)                   AS order_id,
        CAST(DATE(cos.order_date) AS DATE)                  AS order_date,
        CAST(TRIM(cos.customer_id) AS STRING)               AS customer_id,
        CAST(TRIM(cos.customer_email_hash) AS STRING)       AS customer_email_hash,
        CAST(TRIM(cos.order_status) AS STRING)              AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 18))     AS order_total_amount,
        CAST(TRIM(cos.currency_code) AS STRING)             AS currency_code,
        CAST(DATE(cos.ingestion_date) AS DATE)              AS ingestion_date,
        CAST(TRIM(cos.source_system) AS STRING)             AS source_system,
        CAST(TRIM(cos.record_valid_flag) AS STRING)         AS record_valid_flag
    FROM customer_orders_silver cos
    """
)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

job.commit()
```