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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# =============================================================================
# Source Reads + Temp Views
# =============================================================================

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sts")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("pms")

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("sms")

ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
ass_df.createOrReplaceTempView("ass")

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_silver.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("sps")

# =============================================================================
# Target: gold_sales_transactions
# =============================================================================

gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.sale_date) AS sale_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_amount AS DOUBLE) AS total_amount
    FROM sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# =============================================================================
# Target: gold_product_master
# =============================================================================

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand,
        CAST(pms.price AS DOUBLE) AS price,
        CAST(pms.attributes AS STRING) AS attributes
    FROM pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# =============================================================================
# Target: gold_store_master
# =============================================================================

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.region AS STRING) AS region,
        CAST(sms.city AS STRING) AS city,
        CAST(sms.store_area AS STRING) AS store_area
    FROM sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# =============================================================================
# Target: gold_aggregated_sales
# =============================================================================

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(ass.report_date) AS report_date,
        CAST(ass.store_id AS STRING) AS store_id,
        CAST(ass.product_id AS STRING) AS product_id,
        CAST(ass.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(ass.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(ass.average_price AS DOUBLE) AS average_price
    FROM ass
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# =============================================================================
# Target: gold_sales_performance
# =============================================================================

gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.store_id AS STRING) AS store_id,
        CAST(sps.product_id AS STRING) AS product_id,
        CAST(sps.sale_period AS STRING) AS sale_period,
        CAST(sps.sales_growth_rate AS DOUBLE) AS sales_growth_rate,
        CAST(sps.average_discount AS DOUBLE) AS average_discount
    FROM sps
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

job.commit()