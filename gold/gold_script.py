```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# SOURCE: silver.customer_orders_silver
# ------------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# ------------------------------------------------------------------------------------
# TARGET: gold.gold_customer_orders_daily
# - Direct promotion from silver.customer_orders_silver (no joins)
# - Apply explicit casts per target types
# - ROW_NUMBER de-dup by order_id keeping latest load_timestamp
# ------------------------------------------------------------------------------------
gold_customer_orders_daily_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(cos.order_id AS STRING)                 AS order_id,
            CAST(cos.customer_id AS STRING)              AS customer_id,
            CAST(cos.order_date AS DATE)                 AS order_date,
            CAST(cos.order_timestamp AS TIMESTAMP)       AS order_timestamp,
            CAST(cos.order_status AS STRING)             AS order_status,
            CAST(cos.total_order_amount AS DECIMAL(38,18)) AS total_order_amount,
            CAST(cos.currency_code AS STRING)            AS currency_code,
            CAST(cos.item_count AS INT)                  AS item_count,
            CAST(cos.source_file_date AS DATE)           AS source_file_date,
            CAST(cos.load_timestamp AS TIMESTAMP)        AS load_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY cos.order_id
                ORDER BY cos.load_timestamp DESC
            ) AS rn
        FROM customer_orders_silver cos
    )
    SELECT
        order_id,
        customer_id,
        order_date,
        order_timestamp,
        order_status,
        total_order_amount,
        currency_code,
        item_count,
        source_file_date,
        load_timestamp
    FROM ranked
    WHERE rn = 1
    """
)

# ------------------------------------------------------------------------------------
# WRITE: Each target table as a SINGLE CSV file directly under TARGET_PATH
# Output path format: TARGET_PATH + "/" + target_table + ".csv"
# ------------------------------------------------------------------------------------
target_output_path = f"{TARGET_PATH}/gold_customer_orders_daily.csv"

(
    gold_customer_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(target_output_path)
)

job.commit()
```