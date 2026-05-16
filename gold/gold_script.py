import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read sources from S3 (CSV)
# -------------------------------------------------------------------
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
sads_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregates_daily_silver.{FILE_FORMAT}/")
)
dqms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create temp views
# -------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
pms_df.createOrReplaceTempView("pms")
sms_df.createOrReplaceTempView("sms")
sads_df.createOrReplaceTempView("sads")
dqms_df.createOrReplaceTempView("dqms")

# -------------------------------------------------------------------
# Target: gold_sales_data
# -------------------------------------------------------------------
gold_sales_data_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.sale_date AS DATE) AS sale_date
FROM sts
""")

(
    gold_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_data.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING) AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING) AS category,
  CAST(pms.brand AS STRING) AS brand,
  CAST(pms.price AS FLOAT) AS price
FROM pms
""")

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
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING) AS store_id,
  CAST(sms.store_name AS STRING) AS store_name,
  CAST(sms.location AS STRING) AS location,
  CAST(sms.store_size AS STRING) AS store_size
FROM sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_sales_aggregates
# -------------------------------------------------------------------
gold_sales_aggregates_df = spark.sql("""
SELECT
  CAST(sads.date AS DATE) AS date,
  CAST(sads.store_id AS STRING) AS store_id,
  CAST(sads.total_sales AS DOUBLE) AS total_sales,
  CAST(sads.total_units_sold AS INT) AS total_units_sold,
  CAST(sads.average_transaction_value AS DOUBLE) AS average_transaction_value
FROM sads
""")

(
    gold_sales_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregates.csv")
)

# -------------------------------------------------------------------
# Target: gold_data_quality_metrics
# -------------------------------------------------------------------
gold_data_quality_metrics_df = spark.sql("""
SELECT
  CAST(dqms.metric_name AS STRING) AS metric_name,
  CAST(dqms.metric_value AS DOUBLE) AS metric_value,
  CAST(dqms.date_measured AS DATE) AS date_measured
FROM dqms
""")

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)