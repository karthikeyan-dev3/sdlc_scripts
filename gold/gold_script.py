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

# Recommended CSV read options (kept explicit and consistent)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
}

# -----------------------------------------------------------------------------------
# TABLE: gold.gold_customer_orders
# Sources: silver.customer_orders (alias sco)
# -----------------------------------------------------------------------------------

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

# 2) Create temp views
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (EXACT transformations from UDT)
df_gold_customer_orders = spark.sql(
    """
    SELECT
        CAST(sco.order_id AS STRING)        AS order_id,
        CAST(sco.order_date AS DATE)        AS order_date,
        CAST(sco.customer_id AS STRING)     AS customer_id,
        CAST(sco.order_amount AS DECIMAL(38, 18)) AS order_amount,
        CAST(sco.order_status AS STRING)    AS order_status
    FROM customer_orders_silver sco
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH; no subfolders)
(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# -----------------------------------------------------------------------------------
# TABLE: gold.gold_customer_orders_daily
# Sources: silver.customer_orders_daily (alias scod)
# -----------------------------------------------------------------------------------

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
df_customer_orders_daily_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/customer_orders_daily.{FILE_FORMAT}/")
)

# 2) Create temp views
df_customer_orders_daily_silver.createOrReplaceTempView("customer_orders_daily_silver")

# 3) Transform using Spark SQL (EXACT transformations from UDT)
df_gold_customer_orders_daily = spark.sql(
    """
    SELECT
        CAST(scod.order_date AS DATE)             AS order_date,
        CAST(scod.orders_count AS INT)            AS orders_count,
        CAST(scod.total_order_amount AS DECIMAL(38, 18)) AS total_order_amount,
        CAST(scod.avg_order_amount AS DECIMAL(38, 18))   AS avg_order_amount
    FROM customer_orders_daily_silver scod
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH; no subfolders)
(
    df_gold_customer_orders_daily.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders_daily.csv")
)

job.commit()
```