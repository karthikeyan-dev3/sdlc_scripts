```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------------------
# AWS Glue Context
# ---------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# =======================================================================================
# Target Table: silver.customer_orders_daily
# Source: bronze.customer_orders_daily cod
# =======================================================================================

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
cod_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_daily.{FILE_FORMAT}/")
)

# 2) Create temp views
cod_df.createOrReplaceTempView("cod")

# 3) Transform using Spark SQL (including de-dup with ROW_NUMBER)
customer_orders_daily_sql = """
WITH base AS (
    SELECT
        CAST(cod.transaction_id AS STRING)                         AS order_id,
        CAST(cod.transaction_time AS DATE)                         AS order_date,
        CAST(cod.store_id AS STRING)                               AS customer_id,
        CAST('COMPLETED' AS STRING)                                AS order_status,
        CAST(cod.sale_amount AS DECIMAL(38, 10))                   AS total_order_amount,
        cod.transaction_time                                       AS transaction_time
    FROM cod
),
dedup AS (
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        total_order_amount,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY transaction_time DESC
        ) AS rn
    FROM base
)
SELECT
    order_id,
    order_date,
    customer_id,
    order_status,
    total_order_amount
FROM dedup
WHERE rn = 1
"""

scod_df = spark.sql(customer_orders_daily_sql)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    scod_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_daily.csv")
)

# =======================================================================================
# Target Table: silver.customer_orders_load_audit
# Source: bronze.customer_orders_load_audit cola
# =======================================================================================

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
cola_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_load_audit.{FILE_FORMAT}/")
)

# 2) Create temp views
cola_df.createOrReplaceTempView("cola")

# 3) Transform using Spark SQL
customer_orders_load_audit_sql = """
SELECT
    CAST(cola.load_ts AS DATE)                                               AS load_date,
    CAST(cola.min_transaction_time AS DATE)                                  AS source_file_date,
    CAST(cola.source_record_count AS BIGINT)                                 AS records_extracted,
    CAST(cola.loaded_record_count AS BIGINT)                                 AS records_loaded,
    CAST(
        CASE
            WHEN (cola.source_record_count - cola.loaded_record_count) < 0
                THEN 0
            ELSE (cola.source_record_count - cola.loaded_record_count)
        END
        AS BIGINT
    )                                                                        AS records_rejected,
    CAST(cola.load_status AS STRING)                                         AS load_status
FROM cola
"""

scola_df = spark.sql(customer_orders_load_audit_sql)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    scola_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_load_audit.csv")
)

job.commit()
```