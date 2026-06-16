import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Read source tables (Bronze)
# -----------------------------
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

# -----------------------------
# Target: silver.sales_silver
# -----------------------------
sales_silver_sql = """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.transaction_time AS DATE) AS sale_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND CAST(stb.quantity AS DOUBLE) >= 0
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
  FROM base
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE rn = 1
"""
sales_silver_df = spark.sql(sales_silver_sql)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# -----------------------------
# Target: silver.product_silver
# -----------------------------
product_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    CAST(pb.price AS DOUBLE) AS product_price
  FROM products_bronze pb
  WHERE pb.is_active = true
    AND TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        LENGTH(COALESCE(product_name, '')) DESC,
        LENGTH(COALESCE(product_category, '')) DESC,
        COALESCE(product_price, -1.0) DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  product_category,
  product_price
FROM dedup
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# -----------------------------
# Target: silver.store_silver
# -----------------------------
store_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
    TRIM(sb.state) AS store_region,
    sb.open_date AS open_date
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_location,
    store_region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END ASC,
        CASE
          WHEN store_location IS NULL OR TRIM(store_location) = '' THEN 1 ELSE 0
        END ASC,
        open_date DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  store_location,
  store_region
FROM dedup
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# -----------------------------
# Target: silver.sales_aggregated_silver
# -----------------------------
sales_aggregated_silver_sql = """
SELECT
  sts.store_region AS store_region,
  ps.product_category AS product_category,
  ss.sale_date AS sale_date,
  SUM(ss.total_sales_amount) AS total_sales_amount,
  AVG(ss.quantity_sold) AS average_quantity_sold
FROM sales_silver ss
INNER JOIN product_silver ps
  ON ss.product_id = ps.product_id
INNER JOIN store_silver sts
  ON ss.store_id = sts.store_id
GROUP BY
  sts.store_region,
  ps.product_category,
  ss.sale_date
"""
sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()
