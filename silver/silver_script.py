```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------------------------------------------------------------
# AWS Glue boilerplate
# ----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ----------------------------------------------------------------------------------
# Parameters (as provided)
# ----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------------------------------------------------------------
# Read source tables from S3 (STRICT PATH FORMAT)
# ----------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

order_statuses_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_statuses_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

load_lineage_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/load_lineage_bronze.{FILE_FORMAT}/")
)

# ----------------------------------------------------------------------------------
# Create temp views
# ----------------------------------------------------------------------------------
orders_bronze_df.createOrReplaceTempView("orders_bronze")
order_statuses_bronze_df.createOrReplaceTempView("order_statuses_bronze")
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")
load_lineage_bronze_df.createOrReplaceTempView("load_lineage_bronze")

# ----------------------------------------------------------------------------------
# Target table: customer_orders_silver
# - Conformed, de-duplicated to 1 row per order_id
# - Apply EXACT transformations from UDT
# - Use ROW_NUMBER for dedup (latest order_timestamp per order_id)
# ----------------------------------------------------------------------------------
customer_orders_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            ob.order_id,
            ob.order_timestamp,
            ob.order_total_amount,
            osb.status,
            osb.status_timestamp,
            oib.quantity,
            oib.product_id,
            llb.source_event_time
        FROM orders_bronze ob
        LEFT JOIN order_statuses_bronze osb
            ON ob.order_id = osb.order_id
        LEFT JOIN order_items_bronze oib
            ON ob.order_id = oib.order_id
        LEFT JOIN load_lineage_bronze llb
            ON ob.order_id = llb.source_record_id
           AND llb.source_table = 'sales_transactions_raw'
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_timestamp DESC
            ) AS rn
        FROM joined
    )
    SELECT
        -- cos.order_id = ob.order_id
        CAST(order_id AS STRING) AS order_id,

        -- cos.order_date = CAST(ob.order_timestamp AS DATE)
        CAST(order_timestamp AS DATE) AS order_date,

        -- cos.order_status = COALESCE(MAX_BY(osb.status, osb.status_timestamp), 'COMPLETED')
        COALESCE(MAX_BY(status, status_timestamp), 'COMPLETED') AS order_status,

        -- cos.order_total_amount = ob.order_total_amount
        MAX(order_total_amount) AS order_total_amount,

        -- cos.item_count = COALESCE(SUM(oib.quantity), COUNT(oib.product_id), 0)
        COALESCE(SUM(quantity), COUNT(product_id), 0) AS item_count,

        -- cos.s3_file_date = CAST(llb.source_event_time AS DATE)
        CAST(MAX(source_event_time) AS DATE) AS s3_file_date,

        -- cos.load_timestamp = ob.order_timestamp
        MAX(order_timestamp) AS load_timestamp
    FROM ranked
    WHERE rn = 1
    GROUP BY
        order_id,
        CAST(order_timestamp AS DATE)
    """
)

# ----------------------------------------------------------------------------------
# Write output (SINGLE CSV FILE directly under TARGET_PATH, no subfolders)
# ----------------------------------------------------------------------------------
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
```