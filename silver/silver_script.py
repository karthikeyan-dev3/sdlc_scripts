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

# ------------------------------------------------------------
# Read Source Tables (Bronze) and Create Temp Views
# ------------------------------------------------------------

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

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------

sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DOUBLE) AS total_amount
  FROM sales_transactions_bronze stb
),
dedup AS (
  SELECT
    transaction_id,
    transaction_time,
    product_id,
    store_id,
    quantity,
    total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  CAST(transaction_time AS DATE) AS sale_date,
  product_id,
  store_id,
  CASE WHEN quantity < 0 THEN NULL ELSE quantity END AS quantity,
  CASE WHEN total_amount < 0 THEN NULL ELSE total_amount END AS total_amount
FROM dedup
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

# ------------------------------------------------------------
# Target: silver.product_silver
# ------------------------------------------------------------

product_silver_sql = """
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    CAST(pb.price AS DOUBLE) AS price
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id
    ) AS rn
  FROM base
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

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.store_silver
# ------------------------------------------------------------

store_silver_sql = """
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    TRIM(sb.store_type) AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    store_type,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
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

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

job.commit()