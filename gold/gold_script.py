```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
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

# -----------------------------------------------------------------------------------
# Source: silver.customer_orders_silver (cos)
# -----------------------------------------------------------------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# -----------------------------------------------------------------------------------
# Target: gold.gold_customer_orders
# Description:
#   Select from silver.customer_orders_silver (cos) only; map columns directly.
# -----------------------------------------------------------------------------------
gold_customer_orders_df = spark.sql("""
    SELECT
        CAST(cos.order_id AS STRING)                AS order_id,
        CAST(cos.order_date AS DATE)                AS order_date,
        CAST(cos.customer_id AS STRING)             AS customer_id,
        CAST(cos.order_status AS STRING)            AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38,18)) AS order_total_amount,
        CAST(cos.currency_code AS STRING)           AS currency_code,
        CAST(cos.source_system AS STRING)           AS source_system,
        CAST(cos.ingestion_date AS DATE)            AS ingestion_date,
        CAST(cos.load_timestamp AS TIMESTAMP)       AS load_timestamp
    FROM customer_orders_silver cos
""")

# -----------------------------------------------------------------------------------
# Write: SINGLE CSV file directly under TARGET_PATH (no subfolders)
# Output path must be: TARGET_PATH + "/" + target_table.csv
# -----------------------------------------------------------------------------------
tmp_out_gold_customer_orders = f"{TARGET_PATH}/_tmp_gold_customer_orders"

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(tmp_out_gold_customer_orders)
)

# Move the single part file to the required final filename under TARGET_PATH
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

tmp_path = spark._jvm.org.apache.hadoop.fs.Path(tmp_out_gold_customer_orders)
final_path = spark._jvm.org.apache.hadoop.fs.Path(f"{TARGET_PATH}/gold_customer_orders.csv")

# Delete target file if exists (overwrite semantics)
if fs.exists(final_path):
    fs.delete(final_path, True)

# Find the part file and rename to final_path
for status in fs.listStatus(tmp_path):
    name = status.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        fs.rename(status.getPath(), final_path)

# Clean up temp folder
fs.delete(tmp_path, True)

job.commit()
```