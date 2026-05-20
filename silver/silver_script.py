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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# TABLE: sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    stb.product_id AS product_id,
    stb.store_id AS store_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) AS revenue,
    COALESCE(CAST(stb.quantity AS INT), 0) AS quantity,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE
    stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  revenue,
  quantity
FROM base
WHERE
  rn = 1
  AND quantity >= 0
  AND revenue >= 0
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

# ============================================================
# TABLE: products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY
        CASE WHEN LOWER(TRIM(CAST(pb.is_active AS STRING))) = 'true' THEN 1 ELSE 2 END
    ) AS rn
  FROM products_bronze pb
  WHERE
    pb.product_id IS NOT NULL
    AND LOWER(TRIM(CAST(pb.is_active AS STRING))) = 'true'
)
SELECT
  product_id,
  product_name,
  category
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

# ============================================================
# TABLE: stores_silver
# ============================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(sb.store_name) AS store_name,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY CASE WHEN TRIM(sb.store_name) IS NOT NULL THEN 1 ELSE 2 END
    ) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
)
SELECT
  store_id,
  store_name
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

# ============================================================
# TABLE: store_day_summary_silver
# ============================================================
store_day_summary_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.transaction_date AS transaction_date,
  SUM(sts.revenue) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions,
  SUM(sts.quantity) AS total_quantity
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.transaction_date
"""

store_day_summary_silver_df = spark.sql(store_day_summary_silver_sql)
store_day_summary_silver_df.createOrReplaceTempView("store_day_summary_silver")

(
    store_day_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_day_summary_silver.csv")
)

# ============================================================
# TABLE: store_comparison_period_silver
# ============================================================
store_comparison_period_silver_sql = """
SELECT
  sdss.store_id AS store_id,
  sdss.transaction_date AS transaction_date,
  sdss_prev.total_revenue AS comparison_period_revenue,
  sdss_prev.total_transactions AS comparison_period_transactions
FROM store_day_summary_silver sdss
INNER JOIN store_day_summary_silver sdss_prev
  ON sdss.store_id = sdss_prev.store_id
 AND sdss_prev.transaction_date = DATE_ADD(sdss.transaction_date, -7)
"""

store_comparison_period_silver_df = spark.sql(store_comparison_period_silver_sql)
store_comparison_period_silver_df.createOrReplaceTempView("store_comparison_period_silver")

(
    store_comparison_period_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_comparison_period_silver.csv")
)

# ============================================================
# TABLE: data_quality_checks_silver
# ============================================================
data_quality_checks_silver_sql = """
WITH dup AS (
  SELECT
    stb.transaction_id AS transaction_id,
    COUNT(*) AS cnt
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
  GROUP BY stb.transaction_id
),
j AS (
  SELECT
    sts.transaction_id AS record_id,
    'sales_transactions' AS source_table,
    (ps.product_id IS NOT NULL AND ss.store_id IS NOT NULL AND sts.revenue >= 0 AND sts.quantity >= 0 AND sts.transaction_date IS NOT NULL) AS is_valid,
    CASE
      WHEN ps.product_id IS NULL THEN 'missing_product'
      WHEN ss.store_id IS NULL THEN 'missing_store'
      WHEN sts.revenue < 0 THEN 'negative_revenue'
      WHEN sts.quantity < 0 THEN 'negative_quantity'
      WHEN sts.transaction_date IS NULL THEN 'missing_date'
      ELSE 'duplicate_transaction_id'
    END AS error_description,
    CURRENT_DATE AS processing_date,
    d.cnt AS dup_cnt
  FROM sales_transactions_silver sts
  LEFT JOIN products_silver ps
    ON sts.product_id = ps.product_id
  LEFT JOIN stores_silver ss
    ON sts.store_id = ss.store_id
  LEFT JOIN dup d
    ON sts.transaction_id = d.transaction_id
)
SELECT
  record_id,
  source_table,
  is_valid,
  CASE
    WHEN COALESCE(dup_cnt, 0) > 1 THEN 'duplicate_transaction_id'
    ELSE error_description
  END AS error_description,
  processing_date
FROM j
"""

data_quality_checks_silver_df = spark.sql(data_quality_checks_silver_sql)
data_quality_checks_silver_df.createOrReplaceTempView("data_quality_checks_silver")

(
    data_quality_checks_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_checks_silver.csv")
)

# ============================================================
# TABLE: metadata_tracking_silver
# ============================================================
metadata_tracking_silver_sql = """
SELECT
  'products_raw' AS data_source,
  CAST(CAST(NULL AS STRING) AS DATE) AS data_ingestion_date,
  CURRENT_TIMESTAMP AS data_processing_date,
  'raw->bronze->silver: products_bronze,sales_transactions_bronze,stores_bronze -> products_silver,sales_transactions_silver,stores_silver' AS data_lineage,
  'PASS' AS compliance_status
FROM products_bronze pb

UNION ALL

SELECT
  'sales_transactions_raw' AS data_source,
  CAST(stb.transaction_time AS DATE) AS data_ingestion_date,
  CURRENT_TIMESTAMP AS data_processing_date,
  'raw->bronze->silver: products_bronze,sales_transactions_bronze,stores_bronze -> products_silver,sales_transactions_silver,stores_silver' AS data_lineage,
  'PASS' AS compliance_status
FROM sales_transactions_bronze stb

UNION ALL

SELECT
  'stores_raw' AS data_source,
  CAST(CAST(NULL AS STRING) AS DATE) AS data_ingestion_date,
  CURRENT_TIMESTAMP AS data_processing_date,
  'raw->bronze->silver: products_bronze,sales_transactions_bronze,stores_bronze -> products_silver,sales_transactions_silver,stores_silver' AS data_lineage,
  'PASS' AS compliance_status
FROM stores_bronze sb
"""

metadata_tracking_silver_df = spark.sql(metadata_tracking_silver_sql)
metadata_tracking_silver_df.createOrReplaceTempView("metadata_tracking_silver")

(
    metadata_tracking_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/metadata_tracking_silver.csv")
)

job.commit()
