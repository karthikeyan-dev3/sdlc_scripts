import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

asds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_daily_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")
asds_df.createOrReplaceTempView("aggregated_sales_daily_silver")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------------------

# Target: gold_sales_analytics (gsa) from silver.sales_transactions_silver sts
gold_sales_analytics_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        DATE(sts.transaction_date) AS transaction_date,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    """
)

# Target: gold_product_master (gpm) from silver.product_master_silver pms
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand,
        CAST(pms.price AS FLOAT) AS price
    FROM product_master_silver pms
    """
)

# Target: gold_store_master (gsm) from silver.store_master_silver sms
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.region AS STRING) AS region,
        CAST(sms.store_type AS STRING) AS store_type,
        DATE(sms.opening_date) AS opening_date
    FROM store_master_silver sms
    """
)

# Target: gold_aggregated_sales (gas) from silver.aggregated_sales_daily_silver asds
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(asds.store_id AS STRING) AS store_id,
        CAST(asds.product_id AS STRING) AS product_id,
        DATE(asds.transaction_date) AS transaction_date,
        CAST(asds.total_sales AS DOUBLE) AS total_sales,
        CAST(asds.total_quantity AS INT) AS total_quantity
    FROM aggregated_sales_daily_silver asds
    """
)

# ------------------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------------------
(
    gold_sales_analytics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_analytics.csv")
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()