import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# ----------------------------
# Read Source Tables (S3)
# ----------------------------
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------
# products_silver
# ----------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS DOUBLE) AS price,
    COALESCE(pb.is_active, TRUE) AS is_active,
    ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
  FROM products_bronze pb
  WHERE COALESCE(pb.is_active, TRUE) = TRUE
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

(
    products_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ----------------------------
# stores_silver
# ----------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.city) AS city,
    TRIM(sb.state) AS state,
    TRIM(sb.store_type) AS store_type,
    CAST(sb.open_date AS DATE) AS open_date,
    ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM base
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)

(
    stores_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ----------------------------
# sales_transactions_silver
# ----------------------------
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id,
    stb.store_id,
    stb.product_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id, stb.store_id, stb.product_id, stb.transaction_time
      ORDER BY stb.transaction_time
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time
FROM base
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()