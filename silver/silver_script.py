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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
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
# 2) products_silver
#    Columns (UDT): product_id, product_name, product_category, price
# ------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    CAST(pb.price AS FLOAT) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN pb.category IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN pb.price IS NOT NULL THEN 0 ELSE 1 END
    ) AS rn
  FROM products_bronze pb
)
SELECT
  CAST(product_id AS STRING) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(product_category AS STRING) AS product_category,
  CAST(price AS FLOAT) AS price
FROM base
WHERE rn = 1
  AND product_id IS NOT NULL
  AND price IS NULL OR price >= 0
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

# ------------------------------------------------------------
# 3) stores_silver
#    Columns (UDT): store_id, store_name, store_location, store_region
# ------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
    TRIM(sb.state) AS store_region,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN sb.city IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN sb.state IS NOT NULL THEN 0 ELSE 1 END
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  CAST(store_id AS STRING) AS store_id,
  CAST(store_name AS STRING) AS store_name,
  CAST(store_location AS STRING) AS store_location,
  CAST(store_region AS STRING) AS store_region
FROM base
WHERE rn = 1
  AND store_id IS NOT NULL
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

# ------------------------------------------------------------
# 4) sales_transactions_silver
#    Columns (UDT): transaction_id, product_id, store_id, sale_date, quantity_sold, total_sales_amount
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts,
    CAST(stb.transaction_time AS DATE) AS sale_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount
  FROM sales_transactions_bronze stb
  LEFT JOIN products_silver ps
    ON TRIM(stb.product_id) = ps.product_id
  LEFT JOIN stores_silver ss
    ON TRIM(stb.store_id) = ss.store_id
  WHERE ps.product_id IS NOT NULL
    AND ss.store_id IS NOT NULL
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time_ts DESC
    ) AS rn
  FROM joined
)
SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(store_id AS STRING) AS store_id,
  CAST(sale_date AS DATE) AS sale_date,
  CAST(quantity_sold AS INT) AS quantity_sold,
  CAST(total_sales_amount AS DOUBLE) AS total_sales_amount
FROM dedup
WHERE rn = 1
  AND transaction_id IS NOT NULL
  AND product_id IS NOT NULL
  AND store_id IS NOT NULL
  AND (quantity_sold IS NULL OR quantity_sold >= 0)
  AND (total_sales_amount IS NULL OR total_sales_amount >= 0)
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

# ------------------------------------------------------------
# 5) daily_sales_agg_silver
#    Columns (UDT): store_id, product_id, sales_date, total_quantity_sold, total_sales_amount
# ------------------------------------------------------------
daily_sales_agg_silver_sql = """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.sale_date AS DATE) AS sales_date,
  CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
  CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS total_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.sale_date
"""
daily_sales_agg_silver_df = spark.sql(daily_sales_agg_silver_sql)
daily_sales_agg_silver_df.createOrReplaceTempView("daily_sales_agg_silver")

(
    daily_sales_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_agg_silver.csv")
)

# ------------------------------------------------------------
# 6) monthly_sales_perf_silver
#    Columns (UDT): product_id, store_id, month_start, monthly_quantity_sold, monthly_sales_amount
# ------------------------------------------------------------
monthly_sales_perf_silver_sql = """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(DATE_TRUNC('month', sts.sale_date) AS DATE) AS month_start,
  CAST(SUM(sts.quantity_sold) AS INT) AS monthly_quantity_sold,
  CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS monthly_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  sts.product_id,
  sts.store_id,
  DATE_TRUNC('month', sts.sale_date)
"""
monthly_sales_perf_silver_df = spark.sql(monthly_sales_perf_silver_sql)
monthly_sales_perf_silver_df.createOrReplaceTempView("monthly_sales_perf_silver")

(
    monthly_sales_perf_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/monthly_sales_perf_silver.csv")
)

job.commit()