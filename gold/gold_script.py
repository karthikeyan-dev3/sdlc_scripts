```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue job setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
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
# 1) Read source tables from S3 (STRICT PATH FORMAT)
# -----------------------------------------------------------------------------------
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

payments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/payments_silver.{FILE_FORMAT}/")
)

shipments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/shipments_silver.{FILE_FORMAT}/")
)

order_status_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_status_silver.{FILE_FORMAT}/")
)

currencies_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/currencies_silver.{FILE_FORMAT}/")
)

source_files_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/source_files_silver.{FILE_FORMAT}/")
)

ingestions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/ingestions_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
orders_silver_df.createOrReplaceTempView("orders_silver")
customers_silver_df.createOrReplaceTempView("customers_silver")
payments_silver_df.createOrReplaceTempView("payments_silver")
shipments_silver_df.createOrReplaceTempView("shipments_silver")
order_status_silver_df.createOrReplaceTempView("order_status_silver")
currencies_silver_df.createOrReplaceTempView("currencies_silver")
source_files_silver_df.createOrReplaceTempView("source_files_silver")
ingestions_silver_df.createOrReplaceTempView("ingestions_silver")

# -----------------------------------------------------------------------------------
# 3) Transform (Spark SQL) - Target table: gold.gold_customer_orders
#    - Driving table: orders_silver (os)
#    - Joins: as specified in UDT mapping_details
#    - ROW_NUMBER used to deduplicate to one row per order_id (preferring latest ingestion_date)
#    - Apply exact column mappings (gco.col = src.col) and conform to target types using CAST/COALESCE/TRIM/UPPER/LOWER/DATE
# -----------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            os.order_id,
            os.order_date,
            os.customer_id,
            cs.customer_email_masked,
            cs.customer_name,
            oss.order_status,
            os.order_total_amount,
            cur.currency_code,
            ps.payment_method,
            ss.shipping_country,
            ss.shipping_state,
            ss.shipping_city,
            sfs.source_file_format,
            isg.ingestion_date,
            ROW_NUMBER() OVER (
                PARTITION BY os.order_id
                ORDER BY isg.ingestion_date DESC
            ) AS rn
        FROM orders_silver os
        LEFT JOIN customers_silver cs
            ON os.customer_id = cs.customer_id
        LEFT JOIN payments_silver ps
            ON os.order_id = ps.order_id
        LEFT JOIN shipments_silver ss
            ON os.order_id = ss.order_id
        LEFT JOIN order_status_silver oss
            ON os.order_id = oss.order_id
        LEFT JOIN currencies_silver cur
            ON os.currency_code = cur.currency_code
        LEFT JOIN source_files_silver sfs
            ON os.source_file_id = sfs.source_file_id
        LEFT JOIN ingestions_silver isg
            ON os.ingestion_id = isg.ingestion_id
    )
    SELECT
        CAST(TRIM(order_id) AS STRING)                                         AS order_id,
        CAST(DATE(order_date) AS DATE)                                         AS order_date,
        CAST(TRIM(customer_id) AS STRING)                                      AS customer_id,
        CAST(TRIM(COALESCE(customer_email_masked, '')) AS STRING)              AS customer_email_masked,
        CAST(TRIM(COALESCE(customer_name, '')) AS STRING)                      AS customer_name,
        CAST(TRIM(COALESCE(order_status, 'UNKNOWN')) AS STRING)                AS order_status,
        CAST(COALESCE(order_total_amount, 0) AS DECIMAL(38, 10))               AS order_total_amount,
        CAST(TRIM(COALESCE(currency_code, 'UNKNOWN')) AS STRING)               AS currency_code,
        CAST(TRIM(COALESCE(payment_method, 'UNKNOWN')) AS STRING)              AS payment_method,
        CAST(TRIM(COALESCE(shipping_country, 'UNKNOWN')) AS STRING)            AS shipping_country,
        CAST(TRIM(COALESCE(shipping_state, 'UNKNOWN')) AS STRING)              AS shipping_state,
        CAST(TRIM(COALESCE(shipping_city, 'UNKNOWN')) AS STRING)               AS shipping_city,
        CAST(TRIM(COALESCE(source_file_format, 'UNKNOWN')) AS STRING)          AS source_file_format,
        CAST(DATE(ingestion_date) AS DATE)                                     AS ingestion_date
    FROM joined
    WHERE rn = 1
    """
)

# -----------------------------------------------------------------------------------
# 4) Save output (STRICT OUTPUT RULE: single CSV file directly under TARGET_PATH)
#    Output: s3://.../gold_customer_orders.csv
# -----------------------------------------------------------------------------------
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

job.commit()
```