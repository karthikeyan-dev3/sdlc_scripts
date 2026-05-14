
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

# =========================================================
# 1) Read source tables from S3 (bronze)
# =========================================================
transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# =========================================================
# 2) sales_transactions_silver
#    - transaction_date = CAST(tb.transaction_time AS DATE)
#    - quantity_sold = tb.quantity
#    - total_revenue = tb.sale_amount
#    - Dedup: by transaction_id keep latest transaction_time
#    - Filter: transaction_id/store_id/product_id not null
#    - Coerce negative quantity or sale_amount to null
# =========================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    tb.transaction_id AS transaction_id,
    tb.store_id AS store_id,
    tb.product_id AS product_id,
    CAST(tb.transaction_time AS DATE) AS transaction_date,
    CAST(tb.quantity AS INT) AS quantity_sold_raw,
    CAST(tb.sale_amount AS DOUBLE) AS total_revenue_raw,
    tb.transaction_time AS transaction_time
  FROM transactions_bronze tb
  WHERE tb.transaction_id IS NOT NULL
    AND tb.store_id IS NOT NULL
    AND tb.product_id IS NOT NULL
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    CASE WHEN quantity_sold_raw < 0 THEN NULL ELSE quantity_sold_raw END AS quantity_sold,
    CASE WHEN total_revenue_raw < 0 THEN NULL ELSE total_revenue_raw END AS total_revenue,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
  FROM base
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_date,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
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

# =========================================================
# 3) stores_silver
#    - region = COALESCE(NULLIF(TRIM(state), ''), NULLIF(TRIM(city), ''), 'UNKNOWN')
#    - standardize store_name/city/state/store_type with TRIM and consistent casing
#    - Dedup: by store_id keep latest by open_date then store_name
# =========================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(sb.store_name) AS store_name,
    COALESCE(NULLIF(TRIM(sb.state), ''), NULLIF(TRIM(sb.city), ''), 'UNKNOWN') AS region,
    sb.open_date AS open_date
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY CAST(open_date AS DATE) DESC, store_name DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region
FROM dedup
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

# =========================================================
# 4) products_silver
#    - standardize product_name/category with TRIM and consistent casing
#    - ensure category not null default 'UNKNOWN'
#    - De-dup: on product_id (keep active record if multiple, else latest by product_name)
#    Note: only columns provided in UDT are used.
# =========================================================
products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(pb.product_name) AS product_name,
    COALESCE(NULLIF(TRIM(pb.category), ''), 'UNKNOWN') AS category
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name DESC) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category
FROM dedup
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

# =========================================================
# 5) sales_enriched_silver
#    - Inner join ensures referential integrity (drops missing dimension keys)
# =========================================================
sales_enriched_silver_sql = """
SELECT
  sts.transaction_id AS transaction_id,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  sts.transaction_date AS transaction_date,
  sts.quantity_sold AS quantity_sold,
  sts.total_revenue AS total_revenue
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
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

# =========================================================
# 6) data_quality_daily_silver
#    - refresh_date = CURRENT_DATE
#    - data_quality_score = (rule_pass_count / rule_total_count)
#      using rule checks on non-null keys and non-negative qty/revenue
#    - data_refresh_rate = DATEDIFF(CURRENT_DATE, MAX(transaction_date))
# =========================================================
data_quality_daily_silver_sql = """
WITH agg AS (
  SELECT
    COUNT(1) AS row_cnt,
    SUM(CASE WHEN sts.transaction_id IS NOT NULL THEN 1 ELSE 0 END) AS pass_transaction_id,
    SUM(CASE WHEN sts.store_id IS NOT NULL THEN 1 ELSE 0 END) AS pass_store_id,
    SUM(CASE WHEN sts.product_id IS NOT NULL THEN 1 ELSE 0 END) AS pass_product_id,
    SUM(CASE WHEN sts.quantity_sold IS NOT NULL AND sts.quantity_sold >= 0 THEN 1 ELSE 0 END) AS pass_quantity_sold,
    SUM(CASE WHEN sts.total_revenue IS NOT NULL AND sts.total_revenue >= 0 THEN 1 ELSE 0 END) AS pass_total_revenue,
    MAX(sts.transaction_date) AS max_transaction_date
  FROM sales_transactions_silver sts
),
score AS (
  SELECT
    CURRENT_DATE AS refresh_date,
    (
      (
        pass_transaction_id +
        pass_store_id +
        pass_product_id +
        pass_quantity_sold +
        pass_total_revenue
      )
      /
      CAST((row_cnt * 5) AS DOUBLE)
    ) AS data_quality_score,
    DATEDIFF(CURRENT_DATE, max_transaction_date) AS data_refresh_rate
  FROM agg
)
SELECT
  refresh_date,
  data_quality_score,
  data_refresh_rate
FROM score
"""
data_quality_daily_silver_df = spark.sql(data_quality_daily_silver_sql)
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

(
    data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_silver.csv")
)

job.commit()