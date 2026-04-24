```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Job / Context
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Parameters (as provided)
# ----------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# SOURCE: silver.customer_orders_silver (alias: cos)
# Read source table from S3 and create temp view
# ----------------------------
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# ----------------------------
# TARGET: gold.gold_customer_orders (alias: gco)
# Transform using Spark SQL (1:1 mapping as per UDT)
# ----------------------------
gold_customer_orders_sql = """
SELECT
    CAST(cos.order_id AS STRING)                       AS order_id,
    DATE(cos.order_date)                               AS order_date,
    CAST(cos.customer_id AS STRING)                    AS customer_id,
    CAST(cos.order_status AS STRING)                   AS order_status,
    CAST(cos.currency_code AS STRING)                  AS currency_code,
    CAST(cos.order_total_amount AS DECIMAL(38, 18))    AS order_total_amount,
    CAST(cos.source_file_path AS STRING)               AS source_file_path,
    CAST(cos.ingested_at AS TIMESTAMP)                 AS ingested_at
FROM customer_orders_silver cos
"""

gold_customer_orders_df = spark.sql(gold_customer_orders_sql)

# ----------------------------
# WRITE: Single CSV file directly under TARGET_PATH
# Output: s3://.../gold/gold_customer_orders.csv
# ----------------------------
gold_customer_orders_tmp = f"{TARGET_PATH}/__tmp_gold_customer_orders_csv"

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_tmp)
)

# Move the single part file to the required exact path: TARGET_PATH + "/gold_customer_orders.csv"
tmp_files = glueContext._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(tmp_files)
tmp_path = spark._jvm.org.apache.hadoop.fs.Path(gold_customer_orders_tmp)
final_path = spark._jvm.org.apache.hadoop.fs.Path(f"{TARGET_PATH}/gold_customer_orders.csv")

# Find the part file
for status in fs.listStatus(tmp_path):
    name = status.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        part_file_path = status.getPath()
        break

# Delete final if exists, then rename part -> final, then delete tmp folder
if fs.exists(final_path):
    fs.delete(final_path, True)

fs.rename(part_file_path, final_path)
fs.delete(tmp_path, True)

job.commit()
```