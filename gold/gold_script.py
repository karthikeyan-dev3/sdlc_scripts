
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
# 1) Read source tables from S3
# --------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

data_refresh_log_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_refresh_log_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
data_refresh_log_silver_df.createOrReplaceTempView("data_refresh_log_silver")

# --------------------------------------------------------------------
# TARGET: gold_sales_performance
# --------------------------------------------------------------------
gold_sales_performance_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)                    AS transaction_id,
  DATE(sts.transaction_date)                           AS transaction_date,
  CAST(sts.store_id AS STRING)                         AS store_id,
  CAST(ss.store_name AS STRING)                        AS store_name,
  CAST(ss.store_location AS STRING)                    AS store_location,
  CAST(sts.product_id AS STRING)                       AS product_id,
  CAST(ps.product_name AS STRING)                      AS product_name,
  CAST(ps.product_category AS STRING)                  AS product_category,
  CAST(sts.quantity_sold AS INT)                       AS quantity_sold,
  CAST(sts.total_revenue AS DOUBLE)                    AS total_revenue,
  CAST(sts.product_cost AS DOUBLE)                     AS product_cost,
  CAST((sts.total_revenue - sts.product_cost) AS DOUBLE) AS profit_margin
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""

gold_sales_performance_df = spark.sql(gold_sales_performance_sql)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_store_performance
# --------------------------------------------------------------------
gold_store_performance_sql = """
SELECT
  CAST(sts.store_id AS STRING)        AS store_id,
  CAST(ss.store_name AS STRING)       AS store_name,
  CAST(ss.store_location AS STRING)   AS store_location,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)       AS total_items_sold,
  CAST(AVG(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS average_transaction_value
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  sts.store_id,
  ss.store_name,
  ss.store_location
"""

gold_store_performance_df = spark.sql(gold_store_performance_sql)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_product_performance
# --------------------------------------------------------------------
gold_product_performance_sql = """
SELECT
  CAST(sts.product_id AS STRING)      AS product_id,
  CAST(ps.product_name AS STRING)     AS product_name,
  CAST(ps.product_category AS STRING) AS product_category,
  CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)       AS total_units_sold,
  CAST(
    SUM(CAST(sts.total_revenue AS DOUBLE)) / NULLIF(SUM(CAST(sts.quantity_sold AS DOUBLE)), 0)
    AS DOUBLE
  ) AS average_price
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.product_id,
  ps.product_name,
  ps.product_category
"""

gold_product_performance_df = spark.sql(gold_product_performance_sql)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# --------------------------------------------------------------------
# TARGET: gold_data_quality_metrics
# --------------------------------------------------------------------
gold_data_quality_metrics_sql = """
SELECT
  DATE(drls.refresh_date) AS refresh_date,
  CAST(
    (SUM(CASE WHEN drls.success = true THEN 1 ELSE 0 END) * 1.0) / NULLIF(COUNT(1), 0)
    AS DOUBLE
  ) AS data_refresh_rate,
  CAST(
    AVG(CASE WHEN drls.success = true THEN 1.0 ELSE 0.0 END)
    AS DOUBLE
  ) AS data_quality_score
FROM data_refresh_log_silver drls
GROUP BY
  drls.refresh_date
"""

gold_data_quality_metrics_df = spark.sql(gold_data_quality_metrics_sql)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()
