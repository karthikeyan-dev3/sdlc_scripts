import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

dqms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ss_df.createOrReplaceTempView("stores_silver")
ps_df.createOrReplaceTempView("products_silver")
dqms_df.createOrReplaceTempView("data_quality_metrics_silver")

# ============================================================
# Target: gold_sales_transactions
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.revenue AS DOUBLE) AS revenue
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

# ============================================================
# Target: gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(DISTINCT sts.transaction_id) AS DOUBLE) AS transaction_count,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  sts.store_id,
  ss.store_name
"""
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.product_category AS STRING) AS product_category,
  CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS revenue_contribution,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS quantity_sold
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.product_id,
  ps.product_name,
  ps.product_category
"""
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_master_data
# ============================================================
gold_master_data_df = spark.sql(
    """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.product_category AS STRING) AS product_category,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.store_location AS STRING) AS store_location
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""
)

(
    gold_master_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_data.csv")
)

# ============================================================
# Target: gold_data_quality_metrics
# ============================================================
gold_data_quality_metrics_df = spark.sql(
    """
SELECT
  CAST(dqms.metric_id AS STRING) AS metric_id,
  CAST(dqms.metric_name AS STRING) AS metric_name,
  CAST(dqms.value AS DOUBLE) AS value,
  CAST(dqms.target_value AS DOUBLE) AS target_value,
  CAST(dqms.date_measured AS DATE) AS date_measured
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
