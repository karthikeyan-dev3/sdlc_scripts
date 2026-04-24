```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark session
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ======================================================================================
# TARGET TABLE: gold.gold_customer_orders
# Source: silver.orders_silver (alias: os)
# ======================================================================================

# 1) Read source table(s) from S3
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp views
orders_silver_df.createOrReplaceTempView("orders_silver")

# 3) Transform using Spark SQL (apply exact mappings + types)
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(os.order_id AS STRING)           AS order_id,
        CAST(os.order_date AS DATE)           AS order_date,
        CAST(os.customer_id AS STRING)        AS customer_id,
        CAST(os.product_id AS STRING)         AS product_id,
        CAST(os.quantity AS INT)              AS quantity,
        CAST(os.unit_price AS DECIMAL(38,18)) AS unit_price,
        CAST(os.order_amount AS DECIMAL(38,18)) AS order_amount,
        CAST(os.currency_code AS STRING)      AS currency_code,
        CAST(os.order_status AS STRING)       AS order_status,
        CAST(os.ingestion_date AS DATE)       AS ingestion_date,
        CAST(os.loaded_timestamp AS TIMESTAMP) AS loaded_timestamp
    FROM orders_silver os
    """
)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
gold_customer_orders_output_tmp = f"{TARGET_PATH}/_tmp_gold_customer_orders"
gold_customer_orders_output_final = f"{TARGET_PATH}/gold_customer_orders.csv"

(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_output_tmp)
)

# Move the single part file to the required final path (no subfolders)
tmp_files = [f.path for f in dbutils.fs.ls(gold_customer_orders_output_tmp) if f.path.endswith(".csv")]
if len(tmp_files) != 1:
    raise Exception(f"Expected exactly 1 CSV part file, found {len(tmp_files)} at {gold_customer_orders_output_tmp}")

dbutils.fs.rm(gold_customer_orders_output_final, True)
dbutils.fs.cp(tmp_files[0], gold_customer_orders_output_final)
dbutils.fs.rm(gold_customer_orders_output_tmp, True)

job.commit()
```