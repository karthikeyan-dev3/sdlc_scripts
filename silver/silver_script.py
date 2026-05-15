import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------
# 1) READ SOURCE TABLES (BRONZE)
# ------------------------------------------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# 2) product_master_silver
# ------------------------------------------------------------
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS float) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM products_bronze pb
  WHERE
    COALESCE(CAST(pb.is_active AS boolean), false) = true
    AND CAST(pb.price AS float) >= 0
    AND pb.product_id IS NOT NULL
    AND TRIM(pb.product_id) <> ''
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM base
WHERE rn = 1
""")
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------
# 3) store_master_silver
# ------------------------------------------------------------
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    TRIM(sb.state) AS region,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END +
        CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END +
        CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM stores_bronze sb
  WHERE
    sb.store_id IS NOT NULL
    AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_name,
  location,
  region
FROM base
WHERE rn = 1
""")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------
# 4) sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.transaction_time AS date) AS transaction_date,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY
        CASE WHEN stb.transaction_time IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN stb.quantity IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN stb.sale_amount IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON TRIM(stb.product_id) = pms.product_id
  INNER JOIN store_master_silver sms
    ON TRIM(stb.store_id) = sms.store_id
  WHERE
    stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
    AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
    AND CAST(stb.quantity AS int) > 0
    AND CAST(stb.sale_amount AS double) >= 0
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_date,
  quantity_sold,
  total_sales
FROM joined
WHERE rn = 1
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------
# 5) sales_daily_store_silver
# ------------------------------------------------------------
sales_daily_store_silver_df = spark.sql("""
SELECT
  sts.store_id AS store_id,
  sts.transaction_date AS date,
  SUM(sts.total_sales) AS total_revenue,
  SUM(sts.quantity_sold) AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.transaction_date
""")
sales_daily_store_silver_df.createOrReplaceTempView("sales_daily_store_silver")

(
    sales_daily_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_silver.csv")
)

# ------------------------------------------------------------
# 6) sales_daily_product_silver
# ------------------------------------------------------------
sales_daily_product_silver_df = spark.sql("""
SELECT
  sts.product_id AS product_id,
  sts.transaction_date AS date,
  SUM(sts.total_sales) AS total_revenue,
  SUM(sts.quantity_sold) AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  sts.product_id,
  sts.transaction_date
""")
sales_daily_product_silver_df.createOrReplaceTempView("sales_daily_product_silver")

(
    sales_daily_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_product_silver.csv")
)

# ------------------------------------------------------------
# 7) sales_daily_refresh_silver
# ------------------------------------------------------------
sales_daily_refresh_silver_df = spark.sql("""
SELECT
  sts.transaction_date AS refresh_date,
  COUNT(sts.transaction_id) AS records_processed
FROM sales_transactions_silver sts
GROUP BY
  sts.transaction_date
""")
sales_daily_refresh_silver_df.createOrReplaceTempView("sales_daily_refresh_silver")

(
    sales_daily_refresh_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_refresh_silver.csv")
)

job.commit()