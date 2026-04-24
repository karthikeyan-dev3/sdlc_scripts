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

# -----------------------------------------------------------------------------------
# TABLE: gold.gold_customer_orders
# Source: silver.customer_orders_silver cos
# -----------------------------------------------------------------------------------

# 1) Read source table(s) from S3 (STRICT PATH FORMAT)
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (apply UDT mappings + required placeholders)
gold_customer_orders_df = spark.sql("""
    SELECT
        CAST(TRIM(cos.order_id) AS STRING)                                  AS order_id,
        CAST(cos.order_date AS DATE)                                       AS order_date,
        CAST(TRIM(cos.order_status) AS STRING)                              AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 18))                    AS order_total_amount,
        CAST(cos.item_count AS INT)                                        AS item_count,
        CAST(cos.s3_file_date AS DATE)                                     AS s3_file_date,
        CAST(cos.load_timestamp AS TIMESTAMP)                              AS load_timestamp,
        CAST(NULL AS STRING)                                               AS customer_id,     -- placeholder per UDT description
        CAST(NULL AS STRING)                                               AS currency_code    -- placeholder per UDT description
    FROM customer_orders_silver cos
""")

gold_customer_orders_df.createOrReplaceTempView("gold_customer_orders")

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
#    NOTE: coalesce(1) ensures a single data file; we then rename it to *.csv at the target path.
output_tmp_path = f"{TARGET_PATH}/_tmp_gold_customer_orders"

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(output_tmp_path)
)

# Move the single part file to TARGET_PATH/gold_customer_orders.csv (no nesting)
tmp_files = [f.path for f in dbutils.fs.ls(output_tmp_path) if f.path.endswith(".csv")]
if len(tmp_files) != 1:
    raise Exception(f"Expected exactly 1 CSV part file in {output_tmp_path}, found {len(tmp_files)}")

final_output_path = f"{TARGET_PATH}/gold_customer_orders.csv"
dbutils.fs.cp(tmp_files[0], final_output_path, True)
dbutils.fs.rm(output_tmp_path, True)

# -----------------------------------------------------------------------------------
job.commit()
```