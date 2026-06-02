import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# -------------------------------------------------------------------------
# 1) Read source tables
# -------------------------------------------------------------------------
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

dq_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
ss_df.createOrReplaceTempView("ss")
ps_df.createOrReplaceTempView("ps")
dq_df.createOrReplaceTempView("dq")

# -------------------------------------------------------------------------
# 3) Transformations (Spark SQL) + 4) Save outputs (single CSV per target)
# -------------------------------------------------------------------------

# gold.gold_sales_transactions (gst) from silver.sales_transactions_silver (sts)
gold_sales_transactions_df = spark.sql(
    """
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.revenue AS DOUBLE) AS revenue
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

# gold.gold_store_master (gsm) from silver.stores_silver (ss)
gold_store_master_df = spark.sql(
    """
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.region AS STRING) AS region
FROM ss
"""
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# gold.gold_product_master (gpm) from silver.products_silver (ps)
gold_product_master_df = spark.sql(
    """
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.price AS FLOAT) AS price
FROM ps
"""
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# gold.gold_store_performance (gsp) daily aggregates from silver.sales_transactions_silver (sts)
gold_store_performance_df = spark.sql(
    """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(CAST(sts.transaction_id AS STRING)) AS INT) AS total_transactions,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity
FROM sts
GROUP BY
  sts.store_id,
  sts.transaction_date
"""
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# gold.gold_product_performance (gpp) daily aggregates from silver.sales_transactions_silver (sts)
gold_product_performance_df = spark.sql(
    """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS revenue_contribution,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
FROM sts
GROUP BY
  sts.product_id,
  sts.transaction_date
"""
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# gold.gold_data_quality_metrics (gdq) from silver.data_quality_metrics_silver (dq)
gold_data_quality_metrics_df = spark.sql(
    """
SELECT
  CAST(dq.date AS DATE) AS date,
  CAST(dq.total_records AS INT) AS total_records,
  CAST(dq.invalid_records AS INT) AS invalid_records,
  CAST(dq.duplicate_records AS INT) AS duplicate_records,
  CAST(dq.validation_errors AS STRING) AS validation_errors
FROM dq
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
