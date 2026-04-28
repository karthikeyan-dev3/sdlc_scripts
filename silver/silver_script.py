import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source: bronze.order_bronze -> Target: silver.customer_orders_silver
# ------------------------------------------------------------------------------------
order_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_bronze.{FILE_FORMAT}/")
)
order_bronze_df.createOrReplaceTempView("order_bronze")

customer_orders_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(ob.transaction_id AS STRING)                                        AS order_id,
            CAST(ob.transaction_time AS DATE)                                        AS order_date,
            CAST(ob.store_id AS STRING)                                              AS customer_id,
            CAST('COMPLETED' AS STRING)                                              AS order_status,
            CAST(
                CASE
                    WHEN ob.sale_amount IS NULL OR ob.sale_amount < 0 THEN 0
                    ELSE ob.sale_amount
                END AS DECIMAL(38, 10)
            )                                                                        AS order_total_amount,
            CAST('USD' AS STRING)                                                    AS currency_code,
            CAST('sales_transactions_raw' AS STRING)                                 AS source_system,
            CAST(CURRENT_DATE AS DATE)                                               AS ingestion_date,
            ob.transaction_time                                                      AS _transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY ob.transaction_id
                ORDER BY ob.transaction_time DESC
            )                                                                        AS _rn
        FROM order_bronze ob
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        order_total_amount,
        currency_code,
        source_system,
        ingestion_date
    FROM base
    WHERE _rn = 1
    """
)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
