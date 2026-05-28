import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables from S3 + Create Temp Views
# ------------------------------------------------------------------------------

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

spds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_daily_silver.{FILE_FORMAT}/")
)
spds_df.createOrReplaceTempView("sales_performance_daily_silver")

dqms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
dqms_df.createOrReplaceTempView("data_quality_metrics_silver")

# ------------------------------------------------------------------------------
# Target: gold_sales_transactions
# ------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.sale_date AS DATE) AS sale_date,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.total_sale_amount AS DOUBLE) AS total_sale_amount
FROM sales_transactions_silver sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------------------------

gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING) AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING) AS category,
  CAST(pms.price AS DOUBLE) AS price,
  CAST(pms.brand AS STRING) AS brand,
  CAST(pms.attributes AS STRING) AS attributes
FROM product_master_silver pms
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------------------------

gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING) AS store_id,
  CAST(sms.store_name AS STRING) AS store_name,
  CAST(sms.location AS STRING) AS location,
  CAST(sms.opening_date AS DATE) AS opening_date,
  CAST(sms.store_type AS STRING) AS store_type
FROM store_master_silver sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_sales_performance
# ------------------------------------------------------------------------------

gold_sales_performance_df = spark.sql("""
SELECT
  CAST(spds.report_date AS DATE) AS report_date,
  CAST(spds.total_sales AS DOUBLE) AS total_sales,
  CAST(spds.average_transaction_value AS DOUBLE) AS average_transaction_value,
  CAST(spds.total_transactions AS BIGINT) AS total_transactions,
  CAST(spds.region AS STRING) AS region
FROM sales_performance_daily_silver spds
""")

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_data_quality_metrics
# ------------------------------------------------------------------------------

gold_data_quality_metrics_df = spark.sql("""
SELECT
  CAST(dqms.metric_date AS DATE) AS metric_date,
  CAST(dqms.data_freshness_percentage AS DOUBLE) AS data_freshness_percentage,
  CAST(dqms.duplicate_records_count AS BIGINT) AS duplicate_records_count,
  CAST(dqms.data_quality_score AS DOUBLE) AS data_quality_score
FROM data_quality_metrics_silver dqms
""")

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()