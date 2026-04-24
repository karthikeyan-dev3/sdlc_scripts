```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV options (kept explicit and consistent)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "quote": "\"",
    "escape": "\"",
    "multiLine": "false",
}

# ------------------------------------------------------------------------------------
# TABLE: gold.customer_orders_gold
# Sources:
#   silver.customer_orders_silver cos
#   silver.customers_silver cus
#   silver.order_statuses_silver oss  (single-row default via 1=1)
#   silver.currencies_silver cur      (single-row default via 1=1)
# ------------------------------------------------------------------------------------

# 1) Read source tables from S3 (STRICT path format: f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)
order_statuses_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/order_statuses_silver.{FILE_FORMAT}/")
)
currencies_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/currencies_silver.{FILE_FORMAT}/")
)

# 2) Create temp views
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")
customers_silver_df.createOrReplaceTempView("customers_silver")
order_statuses_silver_df.createOrReplaceTempView("order_statuses_silver")
currencies_silver_df.createOrReplaceTempView("currencies_silver")

# 3) Transform using Spark SQL (apply EXACT UDT mappings)
customer_orders_gold_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)                         AS order_id,
        CAST(cos.order_date AS DATE)                         AS order_date,
        CAST(cus.customer_id AS STRING)                      AS customer_id,
        CAST(oss.order_status AS STRING)                     AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 18))      AS order_total_amount,
        CAST(cur.currency_code AS STRING)                    AS currency_code
    FROM customer_orders_silver cos
    LEFT JOIN customers_silver cus
        ON cos.customer_id = cus.customer_id
    LEFT JOIN order_statuses_silver oss
        ON 1 = 1
    LEFT JOIN currencies_silver cur
        ON 1 = 1
    """
)

# 4) Write output (SINGLE CSV file directly under TARGET_PATH; no subfolders)
#    Output path MUST be: TARGET_PATH + "/" + target_table.csv
output_path_customer_orders_gold = f"{TARGET_PATH}/customer_orders_gold.csv"

(
    customer_orders_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(output_path_customer_orders_gold)
)

job.commit()
```