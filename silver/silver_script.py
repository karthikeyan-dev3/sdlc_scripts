
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

# --------------------------------------------------------------------
# Read Source Tables (Bronze)
# --------------------------------------------------------------------
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

# --------------------------------------------------------------------
# Target: silver.products_silver
# --------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    CAST(pb.price AS FLOAT) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(pb.category) IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN pb.price IS NOT NULL AND TRIM(CAST(pb.price AS STRING)) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM base
WHERE rn = 1
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

# --------------------------------------------------------------------
# Target: silver.stores_silver
# --------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(sb.city) IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(sb.state) IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_name,
  location
FROM base
WHERE rn = 1
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

# --------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# --------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(stb.transaction_time AS DATE) AS date,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(COALESCE(CAST(stb.sale_amount AS DOUBLE), CAST(stb.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id), TRIM(stb.store_id), TRIM(stb.product_id)
      ORDER BY
        CASE WHEN stb.transaction_time IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN stb.sale_amount IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN stb.quantity IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  LEFT JOIN products_silver ps
    ON TRIM(stb.product_id) = ps.product_id
  LEFT JOIN stores_silver ss
    ON TRIM(stb.store_id) = ss.store_id
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
)
SELECT
  transaction_id,
  date,
  store_id,
  product_id,
  quantity,
  total_amount
FROM joined
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()