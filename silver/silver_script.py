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

# ------------------------------------------------------------
# 1) Read source tables (Bronze) and create temp views
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
# 2) product_silver
# ------------------------------------------------------------
product_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    CAST(pb.price AS DOUBLE) AS price
  FROM products_bronze pb
  WHERE CAST(pb.price AS DOUBLE) >= 0
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    CAST(price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  CAST(price AS DOUBLE) AS price
FROM dedup
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------
# 3) store_silver
# ------------------------------------------------------------
store_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(TRIM(sb.state) AS STRING) AS state,
    CAST(TRIM(sb.store_type) AS STRING) AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN city IS NOT NULL AND TRIM(city) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN state IS NOT NULL AND TRIM(state) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN store_type IS NOT NULL AND TRIM(store_type) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  CONCAT(COALESCE(city, ''), ', ', COALESCE(state, '')) AS location,
  store_type
FROM dedup
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_silver.csv")
)

# ------------------------------------------------------------
# 4) sales_transactions_clean_silver
# ------------------------------------------------------------
sales_transactions_clean_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS clean_transaction_id,
    CAST(TRIM(stb.product_id) AS STRING) AS clean_product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS clean_store_id,
    CAST(stb.quantity AS INT) AS clean_quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS clean_sales_amount,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS clean_transaction_date,
    CAST(NULL AS STRING) AS clean_customer_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS _transaction_time_ts
  FROM sales_transactions_bronze stb
  WHERE CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
joined AS (
  SELECT
    b.clean_transaction_id,
    b.clean_product_id,
    b.clean_store_id,
    b.clean_quantity_sold,
    b.clean_sales_amount,
    b.clean_transaction_date,
    b.clean_customer_id,
    b._transaction_time_ts,
    ps.product_id AS _valid_product_id,
    ss.store_id AS _valid_store_id
  FROM base b
  LEFT JOIN product_silver ps
    ON TRIM(b.clean_product_id) = ps.product_id
  LEFT JOIN store_silver ss
    ON TRIM(b.clean_store_id) = ss.store_id
),
dedup AS (
  SELECT
    clean_transaction_id,
    clean_product_id,
    clean_store_id,
    clean_quantity_sold,
    clean_sales_amount,
    clean_transaction_date,
    clean_customer_id,
    ROW_NUMBER() OVER (
      PARTITION BY clean_transaction_id
      ORDER BY _transaction_time_ts DESC
    ) AS rn
  FROM joined
)
SELECT
  clean_transaction_id,
  clean_product_id,
  clean_store_id,
  clean_quantity_sold,
  clean_sales_amount,
  clean_transaction_date,
  clean_customer_id
FROM dedup
WHERE rn = 1
"""
sales_transactions_clean_silver_df = spark.sql(sales_transactions_clean_silver_sql)
sales_transactions_clean_silver_df.createOrReplaceTempView("sales_transactions_clean_silver")

(
    sales_transactions_clean_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_clean_silver.csv")
)

# ------------------------------------------------------------
# 5) sales_fact_silver
# ------------------------------------------------------------
sales_fact_silver_sql = """
SELECT
  stcs.clean_transaction_id AS transaction_id,
  stcs.clean_product_id AS product_id,
  stcs.clean_store_id AS store_id,
  stcs.clean_quantity_sold AS quantity_sold,
  stcs.clean_sales_amount AS sales_amount,
  stcs.clean_transaction_date AS transaction_date,
  stcs.clean_customer_id AS customer_id
FROM sales_transactions_clean_silver stcs
"""
sales_fact_silver_df = spark.sql(sales_fact_silver_sql)
sales_fact_silver_df.createOrReplaceTempView("sales_fact_silver")

(
    sales_fact_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_fact_silver.csv")
)

# ------------------------------------------------------------
# 6) daily_sales_agg_silver
# ------------------------------------------------------------
daily_sales_agg_silver_sql = """
SELECT
  CAST(sfs.transaction_date AS DATE) AS date,
  sfs.store_id AS store_id,
  sfs.product_id AS product_id,
  SUM(sfs.quantity_sold) AS total_quantity_sold,
  SUM(sfs.sales_amount) AS total_sales_amount
FROM sales_fact_silver sfs
GROUP BY
  CAST(sfs.transaction_date AS DATE),
  sfs.store_id,
  sfs.product_id
"""
daily_sales_agg_silver_df = spark.sql(daily_sales_agg_silver_sql)
daily_sales_agg_silver_df.createOrReplaceTempView("daily_sales_agg_silver")

(
    daily_sales_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_sales_agg_silver.csv")
)

job.commit()