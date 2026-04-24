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
# SOURCE: silver.customer_orders_silver  (alias: cos)
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# -----------------------------------------------------------------------------------
# TARGET: gold.gold_customer_orders (alias: gco)
# Transformations: EXACT from UDT mappings + required CAST/DATE handling
# Includes ROW_NUMBER-based dedup on order_id (safety) keeping latest order_date
# -----------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql(
    """
    WITH src AS (
        SELECT
            CAST(TRIM(cos.order_id) AS STRING)                          AS order_id,
            DATE(cos.order_date)                                       AS order_date,
            CAST(TRIM(cos.customer_id) AS STRING)                       AS customer_id,
            CAST(cos.order_total_amount AS DECIMAL(38,10))              AS order_total_amount,
            CAST(cos.items_count AS INT)                                AS items_count,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(cos.order_id)
                ORDER BY DATE(cos.order_date) DESC
            ) AS rn
        FROM customer_orders_silver cos
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_total_amount,
        items_count
    FROM src
    WHERE rn = 1
    """
)

# -----------------------------------------------------------------------------------
# WRITE: single CSV file directly under TARGET_PATH (no subfolders)
# Output: s3://.../gold/gold_customer_orders.csv
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