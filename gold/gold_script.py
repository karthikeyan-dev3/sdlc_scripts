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

# ---------------------------------------------------------------------------
# 1) Read source tables from S3
# ---------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
sales_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_summary_silver.{FILE_FORMAT}/")
)
refresh_log_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/refresh_log_silver.{FILE_FORMAT}/")
)

# ---------------------------------------------------------------------------
# 2) Create temp views
# ---------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
sales_summary_silver_df.createOrReplaceTempView("sales_summary_silver")
refresh_log_silver_df.createOrReplaceTempView("refresh_log_silver")

# ---------------------------------------------------------------------------
# 3) Transformations with Spark SQL
# ---------------------------------------------------------------------------

# gold_sales_transactions (gst) from silver.sales_transactions_silver sts
gold_sales_transactions_df = spark.sql("""
SELECT
  sts.transaction_id AS transaction_id,
  sts.product_id AS product_id,
  sts.store_id AS store_id,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.revenue AS DOUBLE) AS revenue,
  DATE(sts.transaction_date) AS transaction_date
FROM sales_transactions_silver sts
""")

# gold_store_metrics (gsm) from sts INNER JOIN ss
gold_store_metrics_df = spark.sql("""
SELECT
  sts.store_id AS store_id,
  SUM(CAST(sts.revenue AS DOUBLE)) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions,
  SUM(CAST(sts.revenue AS DOUBLE)) / COUNT(DISTINCT sts.transaction_id) AS average_transaction_value
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  sts.store_id
""")

# gold_product_performance (gpp) from sts INNER JOIN ps
gold_product_performance_df = spark.sql("""
SELECT
  sts.product_id AS product_id,
  ps.category AS category,
  SUM(CAST(sts.quantity_sold AS INT)) AS units_sold,
  SUM(CAST(sts.revenue AS DOUBLE)) AS total_revenue,
  SUM(CAST(sts.revenue AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS INT)) AS average_price
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.product_id,
  ps.category
""")

# gold_aggregated_reporting (gar) from silver.sales_summary_silver sss
gold_aggregated_reporting_df = spark.sql("""
SELECT
  DATE(sss.report_date) AS report_date,
  CAST(sss.total_sales AS INT) AS total_sales,
  CAST(sss.total_revenue AS DOUBLE) AS total_revenue,
  CAST(sss.total_units_sold AS INT) AS total_units_sold,
  CAST(sss.store_count AS INT) AS store_count,
  CAST(sss.product_count AS INT) AS product_count
FROM sales_summary_silver sss
""")

# gold_refresh_log (grl) from silver.refresh_log_silver rls
gold_refresh_log_df = spark.sql("""
SELECT
  rls.refresh_date AS refresh_date,
  rls.status AS status,
  rls.records_processed AS records_processed,
  rls.success AS success,
  rls.failure_reason AS failure_reason
FROM refresh_log_silver rls
""")

# ---------------------------------------------------------------------------
# 4) Save output (single CSV file per table directly under TARGET_PATH)
# ---------------------------------------------------------------------------
(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

(
    gold_store_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_metrics.csv")
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

(
    gold_aggregated_reporting_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_reporting.csv")
)

(
    gold_refresh_log_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_refresh_log.csv")
)

job.commit()