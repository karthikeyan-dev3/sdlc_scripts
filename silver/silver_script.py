import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables (Bronze)
# ----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ----------------------------
# 2) sales_transactions_silver
# ----------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(UPPER(stb.transaction_id)) AS transaction_id,
    TRIM(UPPER(stb.product_id)) AS product_id,
    TRIM(UPPER(stb.store_id)) AS store_id,
    CAST(stb.transaction_time AS DATE) AS sale_date,
    GREATEST(COALESCE(CAST(stb.quantity AS INT), 0), 0) AS quantity_sold,
    GREATEST(COALESCE(CAST(stb.sale_amount AS DOUBLE), 0D), 0D) AS total_sale_amount,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  WHERE TRIM(UPPER(stb.transaction_id)) IS NOT NULL
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sale_amount,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
  FROM base
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  total_sale_amount
FROM dedup
WHERE rn = 1
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ----------------------------
# 3) product_master_silver
# ----------------------------
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(UPPER(pb.product_id)) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    GREATEST(CAST(pb.price AS DOUBLE), 0D) AS price,
    TRIM(pb.brand) AS brand,
    TO_JSON(NAMED_STRUCT('is_active', pb.is_active)) AS attributes,
    pb.is_active AS is_active
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    brand,
    attributes,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY CASE WHEN is_active = true THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  price,
  brand,
  attributes
FROM dedup
WHERE rn = 1
""")
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_master_silver.csv")
)

# ----------------------------
# 4) store_master_silver
# ----------------------------
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(UPPER(sb.store_id)) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(UPPER(sb.store_type)) AS store_type,
    CONCAT_WS(', ', TRIM(sb.city), TRIM(sb.state)) AS location,
    sb.open_date AS opening_date
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_type,
    location,
    opening_date,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN store_type IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN opening_date IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  store_type,
  location,
  opening_date
FROM dedup
WHERE rn = 1
""")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_master_silver.csv")
)

# ----------------------------
# 5) sales_performance_daily_silver
# ----------------------------
sales_performance_daily_silver_df = spark.sql("""
SELECT
  sts.sale_date AS report_date,
  sms.location AS region,
  SUM(sts.total_sale_amount) AS total_sales,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions,
  CASE
    WHEN COUNT(DISTINCT sts.transaction_id) > 0
      THEN SUM(sts.total_sale_amount) / COUNT(DISTINCT sts.transaction_id)
  END AS average_transaction_value
FROM sales_transactions_silver sts
INNER JOIN store_master_silver sms
  ON sts.store_id = sms.store_id
GROUP BY
  sts.sale_date,
  sms.location
""")
sales_performance_daily_silver_df.createOrReplaceTempView("sales_performance_daily_silver")

(
    sales_performance_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_performance_daily_silver.csv")
)

# ----------------------------
# 6) data_quality_metrics_silver
# ----------------------------
data_quality_metrics_silver_df = spark.sql("""
SELECT
  CURRENT_DATE AS metric_date,
  100D * (
    COUNT(CASE WHEN sts.sale_date >= (CURRENT_DATE - INTERVAL 1 DAY) THEN 1 END)
    / NULLIF(COUNT(*), 0)
  ) AS data_freshness_percentage,
  (COUNT(*) - COUNT(DISTINCT sts.transaction_id)) AS duplicate_records_count,
  100D * (
    COUNT(
      CASE
        WHEN sts.transaction_id IS NOT NULL
         AND sts.product_id IS NOT NULL
         AND sts.store_id IS NOT NULL
         AND sts.sale_date IS NOT NULL
        THEN 1
      END
    ) / NULLIF(COUNT(*), 0)
  ) AS data_quality_score
FROM sales_transactions_silver sts
""")
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()
