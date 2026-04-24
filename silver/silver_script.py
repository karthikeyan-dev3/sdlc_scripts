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
sc = SparkContext()
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

# -----------------------------------------------------------------------------------
# Read source tables from S3 (Bronze)
# IMPORTANT: path must be constructed using FILE_FORMAT and end with "/"
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)

# Create temp views
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# ===================================================================================
# TARGET TABLE: customer_orders_silver
# Mapping: bronze.customer_orders_bronze cob LEFT JOIN bronze.order_items_bronze oib
#          ON cob.transaction_id = oib.transaction_id
# - De-dup by transaction_id keeping latest transaction_time using ROW_NUMBER
# - Apply EXACT transformations from UDT for defined columns
# ===================================================================================
customer_orders_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            cob.transaction_id,
            cob.transaction_time,
            cob.sale_amount,
            oib.transaction_id AS oib_transaction_id
        FROM customer_orders_bronze cob
        LEFT JOIN order_items_bronze oib
            ON cob.transaction_id = oib.transaction_id
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_time,
            sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        -- UDT transformations
        CAST(transaction_id AS STRING)                         AS order_id,
        CAST(transaction_time AS TIMESTAMP)                    AS order_timestamp,
        CAST(CAST(transaction_time AS TIMESTAMP) AS DATE)      AS order_date,
        CAST(sale_amount AS DECIMAL(38, 10))                   AS total_amount
    FROM dedup
    WHERE rn = 1
    """
)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# ===================================================================================
# TARGET TABLE: order_items_silver
# Mapping: bronze.order_items_bronze oib INNER JOIN bronze.customer_orders_bronze cob
#          ON oib.transaction_id = cob.transaction_id
# - De-dup by (transaction_id, product_id) keeping latest cob.transaction_time via ROW_NUMBER
# - Apply EXACT transformations from UDT for defined columns
# ===================================================================================
order_items_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            oib.transaction_id,
            oib.product_id,
            oib.quantity,
            cob.transaction_time
        FROM order_items_bronze oib
        INNER JOIN customer_orders_bronze cob
            ON oib.transaction_id = cob.transaction_id
    ),
    dedup AS (
        SELECT
            transaction_id,
            product_id,
            quantity,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id, product_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        -- UDT transformations
        CAST(transaction_id AS STRING)                         AS order_id,
        CAST(product_id AS STRING)                             AS product_id,
        CAST(quantity AS INT)                                  AS quantity,
        CONCAT(CAST(transaction_id AS STRING), '-', CAST(product_id AS STRING))
                                                             AS order_item_id
    FROM dedup
    WHERE rn = 1
    """
)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

job.commit()
```