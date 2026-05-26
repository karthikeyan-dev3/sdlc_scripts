import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
# Read source tables (bronze)
# -----------------------------
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

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# -----------------------------
# Table: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    CAST(pb.price AS FLOAT) AS price,
    COALESCE(CAST(pb.is_active AS BOOLEAN), TRUE) AS is_active
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    is_active,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 0 ELSE 1 END,
        CASE WHEN category IS NOT NULL AND category <> '' THEN 0 ELSE 1 END,
        CASE WHEN price IS NOT NULL THEN 0 ELSE 1 END
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  price,
  is_active
FROM dedup
WHERE rn = 1
  AND is_active = TRUE
"""
)

products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------
# Table: silver.stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
    ) AS rn
  FROM base
)
SELECT
  store_id
FROM dedup
WHERE rn = 1
"""
)

stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------
# Table: silver.transactions_silver
# -----------------------------
transactions_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(tb.transaction_id) AS transaction_id,
    TRIM(tb.store_id) AS store_id,
    TRIM(tb.product_id) AS product_id,
    CAST(tb.quantity AS INT) AS quantity,
    CAST(tb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(tb.transaction_time AS TIMESTAMP) AS transaction_time
  FROM transactions_bronze tb
  WHERE TRIM(tb.transaction_id) IS NOT NULL AND TRIM(tb.transaction_id) <> ''
    AND TRIM(tb.store_id) IS NOT NULL AND TRIM(tb.store_id) <> ''
    AND TRIM(tb.product_id) IS NOT NULL AND TRIM(tb.product_id) <> ''
),
validated AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time
  FROM base
  WHERE COALESCE(quantity, 0) >= 0
    AND COALESCE(sale_amount, 0D) >= 0D
),
dedup AS (
  SELECT
    v.transaction_id,
    v.store_id,
    v.product_id,
    v.quantity,
    v.sale_amount,
    v.transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY v.transaction_id
      ORDER BY
        CASE WHEN v.transaction_time IS NULL THEN 1 ELSE 0 END,
        v.transaction_time DESC
    ) AS rn
  FROM validated v
)
SELECT
  d.transaction_id,
  d.store_id,
  d.product_id,
  d.quantity,
  d.sale_amount,
  d.transaction_time
FROM dedup d
INNER JOIN stores_silver ss
  ON d.store_id = ss.store_id
INNER JOIN products_silver ps
  ON d.product_id = ps.product_id
WHERE d.rn = 1
"""
)

transactions_silver_df.createOrReplaceTempView("transactions_silver")

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# -----------------------------
# Table: silver.categories_silver
# -----------------------------
categories_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(ps.category) AS category
  FROM products_silver ps
  WHERE TRIM(ps.category) IS NOT NULL AND TRIM(ps.category) <> ''
)
SELECT DISTINCT
  category
FROM base
"""
)

categories_silver_df.createOrReplaceTempView("categories_silver")

(
    categories_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/categories_silver.csv")
)

# -----------------------------
# Table: silver.data_quality_assessments_silver
# -----------------------------
data_quality_assessments_silver_df = spark.sql(
    """
SELECT
  CAST(ts.transaction_time AS DATE) AS assessment_date
FROM transactions_silver ts
"""
)

data_quality_assessments_silver_df.createOrReplaceTempView(
    "data_quality_assessments_silver"
)

(
    data_quality_assessments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_assessments_silver.csv")
)

job.commit()
