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

# -------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# -------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        DATE(sts.sales_date) AS sales_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        CAST(pms.price AS FLOAT) AS price
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.location AS location,
        sms.region AS region
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_sales_aggregated
# -------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        DATE(sas.date) AS date,
        sas.store_id AS store_id,
        sas.product_id AS product_id,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_aggregated_silver sas
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# -------------------------------------------------------------------
# Target: data_quality_metrics
# -------------------------------------------------------------------
data_quality_metrics_df = spark.sql(
    """
    SELECT
        dqms.metric_name AS metric_name,
        CAST(dqms.metric_value AS DOUBLE) AS metric_value,
        dqms.last_updated AS last_updated
    FROM data_quality_metrics_silver dqms
    """
)

(
    data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics.csv")
)

job.commit()