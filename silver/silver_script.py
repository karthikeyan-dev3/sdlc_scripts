```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ===================================================================================
# Target: silver.customer_orders_silver
# Sources: bronze.orders_bronze (alias ob)
# De-dup: by transaction_id keep latest by transaction_time
# ===================================================================================

# 1) Read source tables
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
orders_bronze_df.createOrReplaceTempView("orders_bronze")

# 3) Transform using Spark SQL (incl. ROW_NUMBER de-dup)
customer_orders_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            ob.*,
            ROW_NUMBER() OVER (
                PARTITION BY ob.transaction_id
                ORDER BY ob.transaction_time DESC
            ) AS rn
        FROM orders_bronze ob
        WHERE ob.transaction_id IS NOT NULL
          AND ob.transaction_time IS NOT NULL
          AND ob.store_id IS NOT NULL
          AND ob.sale_amount IS NOT NULL
          AND ob.source_file_path IS NOT NULL
          AND ob.ingested_at IS NOT NULL
    )
    SELECT
        CAST(ob.transaction_id AS STRING)                             AS order_id,
        DATE(ob.transaction_time)                                     AS order_date,
        CAST(ob.store_id AS STRING)                                   AS customer_id,
        CAST('COMPLETED' AS STRING)                                   AS order_status,
        CAST('USD' AS STRING)                                         AS currency_code,
        CAST(ob.sale_amount AS DECIMAL(38, 10))                       AS order_total_amount,
        CAST(ob.source_file_path AS STRING)                           AS source_file_path,
        CAST(ob.ingested_at AS TIMESTAMP)                             AS ingested_at
    FROM ranked ob
    WHERE ob.rn = 1
    """
)

# 4) Save output (single CSV file directly under TARGET_PATH)
#    NOTE: Spark writes to a folder; to produce a single CSV file at the exact key,
#    we coalesce(1) and write to the required path.
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
```