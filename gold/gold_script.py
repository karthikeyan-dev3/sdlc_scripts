
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# 1) READ SOURCE TABLES FROM S3
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2) CREATE TEMP VIEWS
# -------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# -------------------------------------------------------------------
# TARGET TABLE: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_sql = """
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.transaction_time AS TIMESTAMP) AS sales_date,
  CAST(sts.sale_amount AS DOUBLE) AS total_amount
FROM sales_transactions_silver sts
"""

gold_sales_transactions_df = spark.sql(gold_sales_transactions_sql)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -------------------------------------------------------------------
# TARGET TABLE: gold_product_performance
# -------------------------------------------------------------------
gold_product_performance_sql = """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS units_sold,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS revenue
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.product_id,
  ps.product_name,
  ps.category
"""

gold_product_performance_df = spark.sql(gold_product_performance_sql)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# -------------------------------------------------------------------
# TARGET TABLE: gold_store_performance
# -------------------------------------------------------------------
gold_store_performance_sql = """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(CONCAT(CAST(ss.city AS STRING), ', ', CAST(ss.state AS STRING)) AS STRING) AS location,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT) AS customer_count
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  sts.store_id,
  ss.store_name,
  ss.city,
  ss.state
"""

gold_store_performance_df = spark.sql(gold_store_performance_sql)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# -------------------------------------------------------------------
# TARGET TABLE: gold_agg_data_for_reporting
# -------------------------------------------------------------------
gold_agg_data_for_reporting_sql = """
WITH daily_base AS (
  SELECT
    CAST(sts.transaction_time AS TIMESTAMP) AS date,
    sts.transaction_id,
    sts.product_id,
    sts.store_id,
    CAST(sts.sale_amount AS DOUBLE) AS sale_amount,
    CAST(sts.quantity AS INT) AS quantity
  FROM sales_transactions_silver sts
  INNER JOIN products_silver ps
    ON sts.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON sts.store_id = ss.store_id
),
daily_agg AS (
  SELECT
    date,
    CAST(COUNT(transaction_id) AS INT) AS total_sales,
    CAST(SUM(sale_amount) AS DOUBLE) AS total_revenue
  FROM daily_base
  GROUP BY date
),
product_rank AS (
  SELECT
    date,
    product_id,
    SUM(sale_amount) AS product_revenue,
    SUM(quantity) AS product_units,
    ROW_NUMBER() OVER (
      PARTITION BY date
      ORDER BY SUM(sale_amount) DESC, SUM(quantity) DESC, product_id DESC
    ) AS rn
  FROM daily_base
  GROUP BY date, product_id
),
store_rank AS (
  SELECT
    date,
    store_id,
    SUM(sale_amount) AS store_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY date
      ORDER BY SUM(sale_amount) DESC, store_id DESC
    ) AS rn
  FROM daily_base
  GROUP BY date, store_id
)
SELECT
  da.date AS date,
  da.total_sales AS total_sales,
  da.total_revenue AS total_revenue,
  CAST(pr.product_id AS STRING) AS top_product_id,
  CAST(sr.store_id AS STRING) AS top_store_id
FROM daily_agg da
LEFT JOIN product_rank pr
  ON da.date = pr.date AND pr.rn = 1
LEFT JOIN store_rank sr
  ON da.date = sr.date AND sr.rn = 1
"""

gold_agg_data_for_reporting_df = spark.sql(gold_agg_data_for_reporting_sql)

(
    gold_agg_data_for_reporting_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_data_for_reporting.csv")
)

job.commit()