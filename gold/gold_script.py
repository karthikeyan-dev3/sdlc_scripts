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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# --------------------------------------------------------------------

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

sales_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")
)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# --------------------------------------------------------------------
# Target: gold_sales_transactions
# --------------------------------------------------------------------

gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(CAST(sts.sales_date AS STRING)) AS sales_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
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

# --------------------------------------------------------------------
# Target: gold_product_master
# --------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.product_category AS STRING) AS product_category,
        CAST(pms.product_brand AS STRING) AS product_brand
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

# --------------------------------------------------------------------
# Target: gold_store_master
# --------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.store_location AS STRING) AS store_location,
        CAST(sms.store_region AS STRING) AS store_region
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

# --------------------------------------------------------------------
# Target: gold_sales_aggregates
# --------------------------------------------------------------------

gold_sales_aggregates_df = spark.sql(
    """
    SELECT
        DATE(CAST(sas.sales_date AS STRING)) AS sales_date,
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.product_category AS STRING) AS product_category,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_aggregates_silver sas
    """
)

(
    gold_sales_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)

# --------------------------------------------------------------------
# Target: gold_data_quality_metrics
# --------------------------------------------------------------------

gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        DATE(CAST(dqms.metric_date AS STRING)) AS metric_date,
        CAST(dqms.total_transactions AS INT) AS total_transactions,
        CAST(dqms.duplicates_count AS INT) AS duplicates_count,
        CAST(dqms.errors_count AS INT) AS errors_count
    FROM data_quality_metrics_silver dqms
    """
)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()