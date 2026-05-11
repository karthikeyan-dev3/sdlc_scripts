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

# -----------------------------------------------------------------------------------
# Read Source Tables (Bronze)
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# Target: products_silver
# -----------------------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS double) AS price,
    pb.is_active AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_id)
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
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

# -----------------------------------------------------------------------------------
# Target: stores_silver
# -----------------------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.city) AS city,
    TRIM(sb.state) AS state,
    TRIM(sb.store_type) AS store_type,
    CAST(sb.open_date AS date) AS open_date,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY TRIM(sb.store_id)
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date,
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

# -----------------------------------------------------------------------------------
# Target: sales_transactions_silver
# -----------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH dedup AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    stb.transaction_time AS transaction_time,
    CAST(stb.transaction_time AS date) AS transaction_date,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS revenue,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND TRIM(stb.store_id) IS NOT NULL
    AND TRIM(stb.store_id) <> ''
    AND TRIM(stb.product_id) IS NOT NULL
    AND TRIM(stb.product_id) <> ''
    AND CAST(stb.quantity AS int) > 0
    AND CAST(stb.sale_amount AS double) >= 0
)
SELECT
  d.transaction_id,
  d.store_id,
  d.product_id,
  d.transaction_time,
  d.transaction_date,
  d.quantity_sold,
  d.revenue
FROM dedup d
INNER JOIN stores_silver ss
  ON d.store_id = ss.store_id
INNER JOIN products_silver ps
  ON d.product_id = ps.product_id
WHERE d.rn = 1
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
