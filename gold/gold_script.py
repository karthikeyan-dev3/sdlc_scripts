```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark setup
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ======================================================================================
# TABLE: gold.gold_customer_orders_daily
# Source: silver.customer_orders_daily (alias: scod)
# ======================================================================================

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
scod_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_daily.{FILE_FORMAT}/")
)

# 2) Create temp views
scod_df.createOrReplaceTempView("customer_orders_daily")

# 3) Transform using Spark SQL (apply EXACT mappings with required SQL functions)
gold_customer_orders_daily_df = spark.sql(
    """
    SELECT
        CAST(TRIM(scod.order_id) AS STRING)                         AS order_id,
        CAST(DATE(scod.order_date) AS DATE)                        AS order_date,
        CAST(TRIM(scod.customer_id) AS STRING)                     AS customer_id,
        CAST(TRIM(scod.order_status) AS STRING)                    AS order_status,
        CAST(scod.total_order_amount AS DECIMAL(38, 18))           AS total_order_amount
    FROM customer_orders_daily scod
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders_daily.csv")
)

# ======================================================================================
# TABLE: gold.gold_customer_orders_load_audit
# Source: silver.customer_orders_load_audit (alias: scola)
# ======================================================================================

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
scola_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_load_audit.{FILE_FORMAT}/")
)

# 2) Create temp views
scola_df.createOrReplaceTempView("customer_orders_load_audit")

# 3) Transform using Spark SQL (apply EXACT mappings with required SQL functions)
gold_customer_orders_load_audit_df = spark.sql(
    """
    SELECT
        CAST(DATE(scola.load_date) AS DATE)                 AS load_date,
        CAST(DATE(scola.source_file_date) AS DATE)          AS source_file_date,
        CAST(scola.records_extracted AS BIGINT)             AS records_extracted,
        CAST(scola.records_loaded AS BIGINT)                AS records_loaded,
        CAST(scola.records_rejected AS BIGINT)              AS records_rejected,
        CAST(TRIM(scola.load_status) AS STRING)             AS load_status
    FROM customer_orders_load_audit scola
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_load_audit_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders_load_audit.csv")
)

job.commit()
```