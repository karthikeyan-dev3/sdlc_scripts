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

# ============================================================
# 1) Read source tables from S3 (Bronze)
# ============================================================

sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
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

# ============================================================
# 2) Create temp views
# ============================================================

sales_bronze_df.createOrReplaceTempView("sales_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# TARGET TABLE: silver.sales_silver
# ============================================================

sales_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    CAST(sb.transaction_id AS STRING)                                AS transaction_id,
    CAST(CAST(sb.transaction_time AS TIMESTAMP) AS DATE)             AS sale_date,
    CAST(sb.product_id AS STRING)                                    AS product_id,
    CAST(sb.store_id AS STRING)                                      AS store_id,
    CAST(sb.quantity AS INT)                                         AS quantity_sold,
    CAST(sb.sale_amount AS DOUBLE)                                   AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY sb.transaction_id
      ORDER BY CAST(sb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_bronze sb
  WHERE
    sb.transaction_id IS NOT NULL
    AND sb.transaction_time IS NOT NULL
    AND sb.product_id IS NOT NULL
    AND sb.store_id IS NOT NULL
    AND CAST(sb.quantity AS INT) > 0
    AND CAST(sb.sale_amount AS DOUBLE) >= 0
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  quantity_sold,
  total_amount
FROM ranked
WHERE rn = 1
""")

sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# ============================================================
# TARGET TABLE: silver.products_silver
# ============================================================

products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING)                                            AS product_id,
    TRIM(pb.product_name)                                                    AS product_name,
    TRIM(pb.category)                                                        AS category,
    TRIM(pb.brand)                                                           AS brand,
    pb.is_active                                                             AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY
        (CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 1 ELSE 0 END) DESC
    ) AS rn
  FROM products_bronze pb
  WHERE
    pb.product_id IS NOT NULL
    AND pb.is_active = true
)
SELECT
  product_id,
  product_name,
  category,
  brand
FROM base
WHERE rn = 1
""")

products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ============================================================
# TARGET TABLE: silver.stores_silver
# ============================================================

stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.store_id AS STRING)                                            AS store_id,
    TRIM(stb.store_name)                                                    AS store_name,
    CONCAT(TRIM(stb.city), ', ', TRIM(stb.state))                            AS location,
    TRIM(stb.store_type)                                                    AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY stb.store_id
      ORDER BY
        (CASE WHEN stb.store_name IS NOT NULL AND TRIM(stb.store_name) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN stb.city IS NOT NULL AND TRIM(stb.city) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN stb.state IS NOT NULL AND TRIM(stb.state) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN stb.store_type IS NOT NULL AND TRIM(stb.store_type) <> '' THEN 1 ELSE 0 END) DESC
    ) AS rn
  FROM stores_bronze stb
  WHERE stb.store_id IS NOT NULL
)
SELECT
  store_id,
  store_name,
  location,
  store_type
FROM base
WHERE rn = 1
""")

stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# TARGET TABLE: silver.sales_enriched_silver
# ============================================================

sales_enriched_silver_df = spark.sql("""
SELECT
  ss.transaction_id AS transaction_id,
  ss.sale_date      AS sale_date,
  ss.product_id     AS product_id,
  ss.store_id       AS store_id,
  ss.quantity_sold  AS quantity_sold,
  ss.total_amount   AS total_amount
FROM sales_silver ss
INNER JOIN products_silver ps
  ON ss.product_id = ps.product_id
INNER JOIN stores_silver sts
  ON ss.store_id = sts.store_id
""")

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

job.commit()