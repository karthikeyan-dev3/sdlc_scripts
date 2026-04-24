```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
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

read_options = {
    "header": "true",
    "inferSchema": "true",
}

# ===================================================================================
# Target table: gold.gold_customer_orders
# ===================================================================================

# 1) Read source tables from S3 (STRICT PATH FORMAT)
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)
order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
order_statuses_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/order_statuses_silver.{FILE_FORMAT}/")
)
currencies_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/currencies_silver.{FILE_FORMAT}/")
)
order_validations_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/order_validations_silver.{FILE_FORMAT}/")
)

# 2) Create temp views
orders_silver_df.createOrReplaceTempView("orders_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")
products_silver_df.createOrReplaceTempView("products_silver")
order_statuses_silver_df.createOrReplaceTempView("order_statuses_silver")
currencies_silver_df.createOrReplaceTempView("currencies_silver")
order_validations_silver_df.createOrReplaceTempView("order_validations_silver")

# 3) Transform using Spark SQL (including ROW_NUMBER for dedup)
gold_customer_orders_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(os.order_id AS STRING)                                   AS order_id,
        CAST(os.order_date AS DATE)                                   AS order_date,
        CAST(os.customer_id AS STRING)                                AS customer_id,
        CAST(ois.product_id AS STRING)                                AS product_id,
        CAST(ois.quantity AS INT)                                     AS quantity,
        CAST(ps.list_price AS DECIMAL(18,2))                          AS unit_price,
        CAST((ois.quantity * ps.list_price) AS DECIMAL(18,2))         AS order_amount,
        CAST(cur.currency_code AS STRING)                             AS currency_code,
        CAST(oss.order_status AS STRING)                              AS order_status,
        CAST(CURRENT_DATE AS DATE)                                    AS ingestion_date,
        CAST(ovs.is_valid AS BOOLEAN)                                 AS is_valid,
        CAST(ovs.validation_errors AS STRING)                         AS validation_errors,

        ROW_NUMBER() OVER (
          PARTITION BY os.order_id, ois.product_id
          ORDER BY os.order_date DESC
        ) AS rn
      FROM orders_silver os
      INNER JOIN order_items_silver ois
        ON os.order_id = ois.order_id
      LEFT JOIN products_silver ps
        ON ois.product_id = ps.product_id
      LEFT JOIN order_statuses_silver oss
        ON os.order_id = oss.order_id
      LEFT JOIN currencies_silver cur
        ON os.order_id = cur.order_id
      LEFT JOIN order_validations_silver ovs
        ON os.order_id = ovs.order_id
    )
    SELECT
      order_id,
      order_date,
      customer_id,
      product_id,
      quantity,
      unit_price,
      order_amount,
      currency_code,
      order_status,
      ingestion_date,
      is_valid,
      validation_errors
    FROM joined
    WHERE rn = 1
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

job.commit()
```