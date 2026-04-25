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

# Helpful defaults for CSV
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ===================================================================================
# SOURCE: silver.customer_orders_silver  ->  TARGET: gold.gold_customer_orders
# ===================================================================================

# 1) Read source table from S3 (STRICT FORMAT)
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (apply exact mappings; include required CAST/COALESCE/TRIM/UPPER/LOWER/DATE where applicable)
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(TRIM(cos.order_id) AS STRING)                                    AS order_id,
  DATE(cos.order_date)                                                  AS order_date,
  CAST(NULLIF(TRIM(cos.customer_id), '') AS STRING)                     AS customer_id,
  CAST(UPPER(TRIM(cos.order_status)) AS STRING)                         AS order_status,
  CAST(UPPER(TRIM(cos.currency_code)) AS STRING)                        AS currency_code,
  CAST(COALESCE(cos.order_total_amount, 0) AS DECIMAL(38, 10))          AS order_total_amount,
  DATE(cos.ingestion_date)                                              AS ingestion_date
FROM customer_orders_silver cos
""")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH (STRICT FORMAT)
(
    gold_customer_orders_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# SOURCE: silver.customer_order_items_silver  ->  TARGET: gold.gold_customer_order_items
# ===================================================================================

# 1) Read source table from S3 (STRICT FORMAT)
customer_order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_order_items_silver_df.createOrReplaceTempView("customer_order_items_silver")

# 3) Transform using Spark SQL (apply exact mappings; include required CAST/COALESCE/TRIM/UPPER/LOWER/DATE where applicable)
gold_customer_order_items_df = spark.sql("""
SELECT
  CAST(TRIM(cois.order_id) AS STRING)                                   AS order_id,
  CAST(COALESCE(cois.order_item_id, 0) AS INT)                           AS order_item_id,
  CAST(TRIM(cois.product_id) AS STRING)                                  AS product_id,
  CAST(COALESCE(cois.quantity, 0) AS INT)                                AS quantity,
  CAST(COALESCE(cois.unit_price, 0) AS DECIMAL(38, 10))                  AS unit_price,
  CAST(COALESCE(cois.line_amount, 0) AS DECIMAL(38, 10))                 AS line_amount
FROM customer_order_items_silver cois
""")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH (STRICT FORMAT)
(
    gold_customer_order_items_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

# ===================================================================================
# SOURCE: silver.order_load_quality_silver  ->  TARGET: gold.gold_order_load_quality
# ===================================================================================

# 1) Read source table from S3 (STRICT FORMAT)
order_load_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_load_quality_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
order_load_quality_silver_df.createOrReplaceTempView("order_load_quality_silver")

# 3) Transform using Spark SQL (apply exact mappings; include required CAST/COALESCE/TRIM/UPPER/LOWER/DATE where applicable)
gold_order_load_quality_df = spark.sql("""
SELECT
  DATE(olqs.ingestion_date)                                              AS ingestion_date,
  CAST(COALESCE(olqs.total_records, 0) AS INT)                            AS total_records,
  CAST(COALESCE(olqs.valid_records, 0) AS INT)                            AS valid_records,
  CAST(COALESCE(olqs.invalid_records, 0) AS INT)                          AS invalid_records,
  CAST(COALESCE(olqs.error_rate_pct, 0) AS DECIMAL(38, 10))               AS error_rate_pct
FROM order_load_quality_silver olqs
""")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH (STRICT FORMAT)
(
    gold_order_load_quality_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_order_load_quality.csv")
)

job.commit()
```