
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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# Read sources (S3) + Temp Views
# --------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("product_master_silver")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("store_master_silver")

sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_analysis_silver.{FILE_FORMAT}/")
)
sas_df.createOrReplaceTempView("sales_analysis_silver")

ags_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
ags_df.createOrReplaceTempView("aggregated_sales_silver")

# --------------------------------------------------------------------
# Target: gold_sales_transactions
# --------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.sale_date) AS sale_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.sale_amount AS DOUBLE) AS sale_amount
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
# Target: gold_sales_analysis
# --------------------------------------------------------------------
gold_sales_analysis_df = spark.sql(
    """
    SELECT
        DATE(sas.sale_date) AS sale_date,
        CAST(sas.product_id AS STRING) AS product_id,
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.total_quantity AS INT) AS total_quantity,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_analysis_silver sas
    """
)

(
    gold_sales_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_analysis.csv")
)

# --------------------------------------------------------------------
# Target: gold_aggregated_sales
# --------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(ags.sale_date) AS sale_date,
        CAST(ags.product_category AS STRING) AS product_category,
        CAST(ags.store_region AS STRING) AS store_region,
        CAST(ags.total_quantity AS INT) AS total_quantity,
        CAST(ags.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM aggregated_sales_silver ags
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()