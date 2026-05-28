import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

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
# 1) stores_silver
# -----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name
FROM dedup
WHERE rn = 1
""")
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------
# 2) products_silver
# -----------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category
FROM dedup
WHERE rn = 1
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------
# 3) sales_transactions_silver
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS transaction_amount,
    DATE(stb.transaction_time) AS transaction_date,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
),
filtered AS (
  SELECT
    b.transaction_id,
    b.store_id,
    b.product_id,
    b.quantity_sold,
    b.transaction_amount,
    b.transaction_date,
    b.transaction_time
  FROM base b
  INNER JOIN stores_silver ss
    ON b.store_id = ss.store_id
  INNER JOIN products_silver ps
    ON b.product_id = ps.product_id
  WHERE b.transaction_id IS NOT NULL AND TRIM(b.transaction_id) <> ''
    AND b.store_id IS NOT NULL AND TRIM(b.store_id) <> ''
    AND b.product_id IS NOT NULL AND TRIM(b.product_id) <> ''
    AND b.quantity_sold IS NOT NULL AND b.quantity_sold > 0
    AND b.transaction_amount IS NOT NULL AND b.transaction_amount >= 0
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity_sold,
    transaction_amount,
    transaction_date,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity_sold,
  transaction_amount,
  transaction_date
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

# -----------------------------
# 4) aggregated_sales_silver
# -----------------------------
aggregated_sales_silver_df = spark.sql("""
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  sts.transaction_date AS aggregated_date,
  SUM(sts.transaction_amount) AS total_sales,
  SUM(sts.quantity_sold) AS total_quantity
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.transaction_date
""")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()