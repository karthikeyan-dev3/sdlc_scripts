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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------
# 1) Read source tables (Bronze)
# -------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------------------
# 2) products_silver (dedup + cleanse)
# ----------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS product_category,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_id) DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  product_category
FROM base
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ----------------------------------------
# 3) stores_silver (dedup + cleanse)
# ----------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(
      CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS STRING
    ) AS store_location,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY TRIM(sb.store_id) DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  store_location
FROM base
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -------------------------------------------------
# 4) sales_transactions_silver (dedup + cleanse)
# -------------------------------------------------
sales_transactions_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sale_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    stb.transaction_time AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount
  FROM ranked
  WHERE rn = 1
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE
  transaction_id IS NOT NULL
  AND sale_date IS NOT NULL
  AND quantity_sold IS NOT NULL AND quantity_sold >= 0
  AND total_sales_amount IS NOT NULL AND total_sales_amount >= 0
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ----------------------------------------
# 5) sales_enriched_silver (joins)
# ----------------------------------------
sales_enriched_silver_sql = """
SELECT
  sts.transaction_id AS transaction_id,
  sts.product_id AS product_id,
  sts.store_id AS store_id,
  sts.sale_date AS sale_date,
  sts.quantity_sold AS quantity_sold,
  sts.total_sales_amount AS total_sales_amount,
  ps.product_name AS product_name,
  ps.product_category AS product_category,
  ss.store_name AS store_name,
  ss.store_location AS store_location
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""
sales_enriched_silver_df = spark.sql(sales_enriched_silver_sql)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

# ----------------------------------------
# 6) store_sales_daily_silver (agg)
# ----------------------------------------
store_sales_daily_silver_sql = """
SELECT
  ses.store_id AS store_id,
  ses.store_name AS store_name,
  ses.store_location AS store_location,
  ses.sale_date AS date,
  SUM(ses.total_sales_amount) AS total_sales,
  COUNT(DISTINCT ses.transaction_id) AS number_of_transactions
FROM sales_enriched_silver ses
GROUP BY
  ses.store_id,
  ses.store_name,
  ses.store_location,
  ses.sale_date
"""
store_sales_daily_silver_df = spark.sql(store_sales_daily_silver_sql)
store_sales_daily_silver_df.createOrReplaceTempView("store_sales_daily_silver")

(
    store_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_sales_daily_silver.csv")
)

# ----------------------------------------
# 7) product_sales_daily_silver (agg)
# ----------------------------------------
product_sales_daily_silver_sql = """
SELECT
  ses.product_id AS product_id,
  ses.product_name AS product_name,
  ses.product_category AS product_category,
  ses.sale_date AS date,
  SUM(ses.total_sales_amount) AS total_sales,
  SUM(ses.quantity_sold) AS quantity_sold
FROM sales_enriched_silver ses
GROUP BY
  ses.product_id,
  ses.product_name,
  ses.product_category,
  ses.sale_date
"""
product_sales_daily_silver_df = spark.sql(product_sales_daily_silver_sql)
product_sales_daily_silver_df.createOrReplaceTempView("product_sales_daily_silver")

(
    product_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_sales_daily_silver.csv")
)

# ----------------------------------------
# 8) sales_daily_summary_silver (agg)
# ----------------------------------------
sales_daily_summary_silver_sql = """
SELECT
  sts.sale_date AS date,
  SUM(sts.total_sales_amount) AS total_sales,
  SUM(sts.quantity_sold) AS total_quantity_sold,
  COUNT(DISTINCT sts.transaction_id) AS number_of_transactions
FROM sales_transactions_silver sts
GROUP BY
  sts.sale_date
"""
sales_daily_summary_silver_df = spark.sql(sales_daily_summary_silver_sql)
sales_daily_summary_silver_df.createOrReplaceTempView("sales_daily_summary_silver")

(
    sales_daily_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_summary_silver.csv")
)

# -------------------------------------------------------
# 9) data_quality_metrics_silver (daily DQ metrics)
# -------------------------------------------------------
data_quality_metrics_silver_sql = """
SELECT
  sts.sale_date AS date,
  COUNT(sts.transaction_id) AS total_records_processed,
  (COUNT(sts.transaction_id) - COUNT(DISTINCT sts.transaction_id)) AS duplicates_removed
FROM sales_transactions_silver sts
GROUP BY
  sts.sale_date
"""
data_quality_metrics_silver_df = spark.sql(data_quality_metrics_silver_sql)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()