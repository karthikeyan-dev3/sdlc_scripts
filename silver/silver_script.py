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
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# TABLE: silver.customer_orders_silver
# -----------------------------------------------------------------------------------

# 1) Read source tables from S3 (strict path format: {SOURCE_PATH}/table_name.{FILE_FORMAT}/)
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

order_status_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_status_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")
order_status_bronze_df.createOrReplaceTempView("order_status_bronze")

# 3) Spark SQL transformation (EXACT transformations from UDT)
customer_orders_silver_df = spark.sql(
    """
    SELECT
        TRIM(cob.order_id)                                         AS order_id,
        CAST(MIN(cob.order_timestamp) AS DATE)                     AS order_date,
        SUM(cob.sale_amount)                                       AS order_total_amount,
        COALESCE(MAX(osb.status), 'COMPLETED')                     AS order_status,
        CASE
            WHEN TRIM(cob.order_id) IS NOT NULL
             AND CAST(MIN(cob.order_timestamp) AS DATE) IS NOT NULL
             AND SUM(cob.sale_amount) IS NOT NULL
            THEN 'Y' ELSE 'N'
        END                                                        AS record_valid_flag
    FROM customer_orders_bronze cob
    LEFT JOIN order_status_bronze osb
        ON cob.order_id = osb.order_id
    GROUP BY
        TRIM(cob.order_id)
    """
)

# 4) Save output as SINGLE CSV directly under TARGET_PATH (no subfolders)
#    Write to a temporary directory then move/rename to {TARGET_PATH}/customer_orders_silver.csv
tmp_out_dir = f"{TARGET_PATH}/__tmp_customer_orders_silver_csv_out"
final_out_file = f"{TARGET_PATH}/customer_orders_silver.csv"

# Ensure single partition -> single CSV part file
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(tmp_out_dir)
)

# Move single part file to the required final path (no subfolders)
jvm = spark._jvm
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_out_dir)
final_path = jvm.org.apache.hadoop.fs.Path(final_out_file)

# Delete final file if it exists (overwrite semantics)
if fs.exists(final_path):
    fs.delete(final_path, True)

# Find the single part file under the temp directory
files = fs.listStatus(tmp_path)
part_file_path = None
for f in files:
    name = f.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        part_file_path = f.getPath()
        break

if part_file_path is None:
    raise Exception(f"No part CSV file found in temporary output directory: {tmp_out_dir}")

# Rename/move part file to final {TARGET_PATH}/customer_orders_silver.csv
fs.rename(part_file_path, final_path)

# Cleanup temp output dir
fs.delete(tmp_path, True)

job.commit()
```