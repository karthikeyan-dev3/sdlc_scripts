import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# Read Source Tables (Bronze)
# =========================
product_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_bronze.{FILE_FORMAT}/")
)
product_bronze_df.createOrReplaceTempView("product_bronze")

store_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_bronze.{FILE_FORMAT}/")
)
store_bronze_df.createOrReplaceTempView("store_bronze")

sales_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)
sales_bronze_df.createOrReplaceTempView("sales_bronze")

# =========================
# product_silver
# =========================
product_silver_sql = """
WITH base AS (
  SELECT
    upper(trim(pb.product_id)) AS product_id,
    coalesce(pb.product_name, 'UNKNOWN') AS product_name,
    coalesce(pb.category, 'UNKNOWN') AS category,
    cast(pb.price as double) AS price
  FROM product_bronze pb
  WHERE pb.product_id IS NOT NULL
),
filtered AS (
  SELECT *
  FROM base
  WHERE price IS NOT NULL
    AND price > 0
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    row_number() OVER (
      PARTITION BY product_id
      ORDER BY product_name DESC, category DESC, price DESC
    ) AS rn
  FROM filtered
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM dedup
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# =========================
# store_silver
# =========================
store_silver_sql = """
WITH base AS (
  SELECT
    upper(trim(sb.store_id)) AS store_id,
    coalesce(sb.store_name, 'UNKNOWN') AS store_name,
    concat_ws(', ', coalesce(sb.city, 'UNKNOWN'), coalesce(sb.state, 'UNKNOWN')) AS location,
    upper(trim(coalesce(sb.store_type, 'UNKNOWN'))) AS store_type
  FROM store_bronze sb
  WHERE sb.store_id IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    store_type,
    row_number() OVER (
      PARTITION BY store_id
      ORDER BY store_name DESC, location DESC, store_type DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  location,
  store_type
FROM dedup
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# =========================
# sales_silver
# =========================
sales_silver_sql = """
WITH base AS (
  SELECT
    upper(trim(slsb.transaction_id)) AS sales_id,
    cast(slsb.transaction_time as date) AS transaction_date,
    upper(trim(slsb.store_id)) AS store_id,
    upper(trim(slsb.product_id)) AS product_id,
    greatest(cast(coalesce(slsb.quantity, 0) as int), 0) AS quantity_sold,
    greatest(cast(coalesce(slsb.sale_amount, 0) as double), 0) AS revenue,
    slsb.transaction_time AS transaction_time
  FROM sales_bronze slsb
  WHERE slsb.transaction_id IS NOT NULL
    AND slsb.store_id IS NOT NULL
    AND slsb.product_id IS NOT NULL
),
ri AS (
  SELECT
    b.sales_id,
    b.transaction_date,
    b.store_id,
    b.product_id,
    b.quantity_sold,
    b.revenue,
    b.transaction_time
  FROM base b
  INNER JOIN store_silver ss
    ON b.store_id = ss.store_id
  INNER JOIN product_silver ps
    ON b.product_id = ps.product_id
),
dedup AS (
  SELECT
    sales_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    revenue,
    row_number() OVER (
      PARTITION BY sales_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM ri
)
SELECT
  sales_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  revenue
FROM dedup
WHERE rn = 1
"""
sales_silver_df = spark.sql(sales_silver_sql)

(
    sales_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)
