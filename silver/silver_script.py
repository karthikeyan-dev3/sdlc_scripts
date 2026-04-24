```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# TABLE: silver.customer_orders_silver
# Sources:
#   - bronze.customer_orders_bronze (alias: cob)
# Dedup:
#   QUALIFY ROW_NUMBER() OVER (PARTITION BY cob.transaction_id ORDER BY cob.transaction_time DESC) = 1
# -----------------------------------------------------------------------------------

# 1) Read source tables from S3
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

# 3) Transform using Spark SQL (including ROW_NUMBER dedup)
customer_orders_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            cob.*,
            ROW_NUMBER() OVER (
                PARTITION BY cob.transaction_id
                ORDER BY cob.transaction_time DESC
            ) AS rn
        FROM customer_orders_bronze cob
    )
    SELECT
        -- cos.order_id = cob.transaction_id
        CAST(cob.transaction_id AS STRING) AS order_id,

        -- cos.order_date = CAST(cob.transaction_time AS DATE)
        CAST(cob.transaction_time AS DATE) AS order_date,

        -- cos.customer_id = cob.store_id
        CAST(cob.store_id AS STRING) AS customer_id,

        -- cos.order_total_amount = cob.sale_amount
        CAST(cob.sale_amount AS DECIMAL(38, 10)) AS order_total_amount,

        -- cos.items_count = cob.quantity
        CAST(cob.quantity AS INT) AS items_count
    FROM ranked cob
    WHERE cob.rn = 1
      AND cob.transaction_id IS NOT NULL
    """
)

# 4) Write target table as a SINGLE CSV file directly under TARGET_PATH
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
```