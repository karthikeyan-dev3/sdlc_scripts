import sys
from awsglue.transforms import *
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

# -------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# -------------------------------------------------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------------------------------------------------
# 2) product_master_silver
#    Columns only: product_id, product_name, category, price
#    - trim/standardize name/category
#    - normalize price to non-negative numeric
#    - dedup by product_id using ROW_NUMBER (no ordering column provided)
# -------------------------------------------------------------------
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    CAST(pb.price AS FLOAT) AS price
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
),
normalized AS (
  SELECT
    product_id,
    product_name,
    category,
    CASE
      WHEN price IS NULL THEN NULL
      WHEN price < 0 THEN CAST(-price AS FLOAT)
      ELSE price
    END AS price
  FROM base
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM normalized
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM dedup
WHERE rn = 1
""")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# -------------------------------------------------------------------
# 3) store_master_silver
#    Columns only: store_id, store_name, region, address
#    - region = state
#    - address = city
#    - standardize store_name
#    - dedup by store_id using ROW_NUMBER (no ordering column provided)
# -------------------------------------------------------------------
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.state) AS region,
    TRIM(sb.city) AS address
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    address,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region,
  address
FROM dedup
WHERE rn = 1
""")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------------------------------------------------
# 4) sales_transactions_silver
#    Columns only: transaction_id, store_id, product_id, transaction_date, revenue, quantity_sold
#    - transaction_date = CAST(transaction_time AS DATE)
#    - revenue = sale_amount
#    - quantity_sold = quantity
#    - enforce non-null transaction_id/store_id/product_id
#    - remove negative/null revenue or quantity
#    - keep only valid product_id/store_id via joins to silver masters
#    - dedup by transaction_id using ROW_NUMBER (no ordering column provided)
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(stb.sale_amount AS DOUBLE) AS revenue,
    CAST(stb.quantity AS INT) AS quantity_sold
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON TRIM(stb.product_id) = pms.product_id
  INNER JOIN store_master_silver sms
    ON TRIM(stb.store_id) = sms.store_id
  WHERE
    stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
),
filtered AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    revenue,
    quantity_sold
  FROM joined
  WHERE
    revenue IS NOT NULL AND revenue >= 0
    AND quantity_sold IS NOT NULL AND quantity_sold >= 0
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    revenue,
    quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_date,
  revenue,
  quantity_sold
FROM dedup
WHERE rn = 1
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -------------------------------------------------------------------
# 5) sales_performance_silver
#    Columns only: store_id, product_id, total_revenue, total_quantity_sold, average_price
#    - group by store_id, product_id
# -------------------------------------------------------------------
sales_performance_silver_df = spark.sql("""
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  CAST(SUM(sts.revenue) AS DOUBLE) AS total_revenue,
  CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
  CAST(
    CASE
      WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.revenue) / SUM(sts.quantity_sold)
      ELSE NULL
    END AS DOUBLE
  ) AS average_price
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id
""")

(
    sales_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_performance_silver.csv")
)

sales_performance_silver_df.createOrReplaceTempView("sales_performance_silver")

# -------------------------------------------------------------------
# 6) metadata_silver
#    Only column provided in UDT: record_count
# -------------------------------------------------------------------
metadata_silver_df = spark.sql("""
SELECT CAST(COUNT(pb.product_id) AS INT) AS record_count
FROM products_bronze pb
WHERE pb.product_id IS NOT NULL
UNION ALL
SELECT CAST(COUNT(sb.store_id) AS INT) AS record_count
FROM stores_bronze sb
WHERE sb.store_id IS NOT NULL
UNION ALL
SELECT CAST(COUNT(stb.transaction_id) AS INT) AS record_count
FROM sales_transactions_bronze stb
WHERE stb.transaction_id IS NOT NULL
""")

(
    metadata_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/metadata_silver.csv")
)

job.commit()
