```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# TABLE: gold.gold_customer_orders
# Source: silver.order_silver (alias os)
# -----------------------------------------------------------------------------------

# 1) Read source table(s) from S3 (STRICT FORMAT)
order_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_silver.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
order_silver_df.createOrReplaceTempView("order_silver")

# 3) Transform using Spark SQL (apply EXACT mappings from UDT)
gold_customer_orders_df = spark.sql("""
SELECT
    CAST(os.order_id AS STRING)                 AS order_id,
    CAST(os.order_date AS TIMESTAMP)            AS order_date,
    CAST(os.product_id AS STRING)               AS product_id,
    CAST(os.quantity AS INT)                    AS quantity,
    CAST(os.unit_price AS DECIMAL(38,18))       AS unit_price,
    CAST(os.order_total_amount AS DECIMAL(38,18)) AS order_total_amount,
    CAST(os.record_hash AS STRING)              AS record_hash,
    CAST(os.is_valid_record AS BOOLEAN)         AS is_valid_record,
    CAST(os.validation_error_reason AS STRING)  AS validation_error_reason
FROM order_silver os
""")

# 4) Save output as SINGLE CSV file directly under TARGET_PATH (no subfolders)
output_path_gold_customer_orders = f"{TARGET_PATH}/gold_customer_orders.csv"

(
    gold_customer_orders_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(output_path_gold_customer_orders)
)

job.commit()
```