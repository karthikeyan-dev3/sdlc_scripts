import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# =============================================================================
# Target Table: gold.gold_customer_orders
# Source: silver.customer_orders_silver (alias: cos)
# =============================================================================

# 1) Read source table from S3
customer_orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
customer_orders_silver_df.createOrReplaceTempView("customer_orders_silver")

# 3) Transform using Spark SQL (apply exact mappings)
gold_customer_orders_df = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)                       AS order_id,
        DATE(cos.order_date)                               AS order_date,
        CAST(cos.customer_id AS STRING)                    AS customer_id,
        CAST(cos.order_status AS STRING)                   AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38,10))     AS order_total_amount,
        CAST(cos.currency_code AS STRING)                  AS currency_code,
        CAST(cos.source_system AS STRING)                  AS source_system,
        DATE(cos.ingestion_date)                           AS ingestion_date
    FROM customer_orders_silver cos
    """
)

# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

job.commit()