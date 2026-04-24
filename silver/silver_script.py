```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# TABLE: silver.customer_orders_silver
# Sources:
#   bronze.orders_bronze (alias: ob) with dedup:
#     ROW_NUMBER() OVER (PARTITION BY ob.transaction_id ORDER BY ob.transaction_time DESC) = 1
# -----------------------------------------------------------------------------------

# 1) Read source table(s) from S3
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)

# 2) Create temp view(s)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

# 3) Transformation SQL (Spark SQL with ROW_NUMBER for dedup)
customer_orders_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            ob.*,
            ROW_NUMBER() OVER (
                PARTITION BY ob.transaction_id
                ORDER BY ob.transaction_time DESC
            ) AS rn
        FROM orders_bronze ob
    )
    SELECT
        -- cos.order_id = ob.transaction_id
        CAST(d.transaction_id AS STRING) AS order_id,

        -- cos.order_date = CAST(ob.transaction_time AS DATE)
        CAST(d.transaction_time AS DATE) AS order_date,

        -- cos.customer_id = NULL
        CAST(NULL AS STRING) AS customer_id,

        -- cos.order_status = 'UNKNOWN'
        CAST('UNKNOWN' AS STRING) AS order_status,

        -- cos.order_total_amount = ob.sale_amount
        CAST(d.sale_amount AS DECIMAL(38, 18)) AS order_total_amount,

        -- cos.currency_code = NULL
        CAST(NULL AS STRING) AS currency_code,

        -- cos.source_system = 'bronze.orders_bronze'
        CAST('bronze.orders_bronze' AS STRING) AS source_system,

        -- cos.ingestion_date = CURRENT_DATE
        CURRENT_DATE() AS ingestion_date,

        -- cos.load_timestamp = CURRENT_TIMESTAMP
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM dedup d
    WHERE d.rn = 1
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)
```