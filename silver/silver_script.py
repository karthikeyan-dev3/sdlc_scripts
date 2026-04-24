```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# TABLE: orders_silver
# Sources: bronze.orders_bronze, bronze.products_bronze
# Join: bronze.orders_bronze ob LEFT JOIN bronze.products_bronze pb ON ob.product_id = pb.product_id
# -----------------------------------------------------------------------------------

# 1) Read source tables from S3 (STRICT PATH FORMAT)
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
orders_bronze_df.createOrReplaceTempView("orders_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# 3) Transform using Spark SQL (apply EXACT UDT transformations + ROW_NUMBER de-dup)
orders_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            ob.transaction_id                                   AS order_id,
            CAST(ob.transaction_time AS DATE)                   AS order_date,
            ob.store_id                                         AS customer_id,
            ob.product_id                                       AS product_id,
            CAST(ob.quantity AS INT)                            AS quantity,
            CAST(pb.price AS DECIMAL(38, 10))                   AS unit_price,
            CAST(COALESCE(ob.quantity * pb.price, ob.sale_amount) AS DECIMAL(38, 10)) AS order_amount,
            'USD'                                               AS currency_code,
            'COMPLETED'                                         AS order_status,
            CAST(ob.transaction_time AS DATE)                   AS ingestion_date,
            ob.transaction_time                                 AS loaded_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY ob.transaction_id
                ORDER BY ob.transaction_time DESC
            ) AS rn
        FROM orders_bronze ob
        LEFT JOIN products_bronze pb
            ON ob.product_id = pb.product_id
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        unit_price,
        order_amount,
        currency_code,
        order_status,
        ingestion_date,
        loaded_timestamp
    FROM base
    WHERE rn = 1
    """
)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
#    Write to a temp dir, then move the single part file to TARGET_PATH/<table>.csv
tmp_dir = f"{TARGET_PATH}/__tmp_orders_silver_csv/"
final_output_path = f"{TARGET_PATH}/orders_silver.csv"

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(tmp_dir)
)

# Move single part file to final location, and clean temp
jvm = spark._jvm
hconf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)

tmp_path = jvm.org.apache.hadoop.fs.Path(tmp_dir)
final_path = jvm.org.apache.hadoop.fs.Path(final_output_path)

# Delete final file if exists (overwrite semantics for the single-file target)
if fs.exists(final_path):
    fs.delete(final_path, True)

# Find the single part file and rename to <table>.csv at TARGET_PATH root
for status in fs.listStatus(tmp_path):
    name = status.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        fs.rename(status.getPath(), final_path)

# Remove temp directory
fs.delete(tmp_path, True)

job.commit()
```