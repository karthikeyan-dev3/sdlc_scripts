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

# -----------------------------
# Read source tables (Bronze)
# -----------------------------
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

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# -----------------------------
# products_silver
# -----------------------------
products_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(pb.price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        TRIM(pb.product_name) DESC,
        TRIM(pb.category) DESC,
        CAST(pb.price AS DOUBLE) DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM ranked
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

# -----------------------------
# stores_silver
# -----------------------------
stores_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(CONCAT(TRIM(sb.city), ',', TRIM(sb.state)) AS STRING) AS location,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        TRIM(sb.store_name) DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  location
FROM ranked
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

# -----------------------------
# transactions_silver
# -----------------------------
transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(tb.transaction_id) AS STRING) AS transaction_id,
    CAST(tb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(tb.transaction_time AS DATE) AS date,
    CAST(TRIM(tb.store_id) AS STRING) AS store_id,
    CAST(TRIM(tb.product_id) AS STRING) AS product_id,
    CAST(tb.quantity AS INT) AS quantity_sold,
    CAST(tb.sale_amount AS DOUBLE) AS total_sales_amount
  FROM transactions_bronze tb
),
dedup AS (
  SELECT
    transaction_id,
    date,
    store_id,
    product_id,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  d.transaction_id,
  d.date,
  d.store_id,
  d.product_id,
  d.quantity_sold,
  d.total_sales_amount
FROM dedup d
LEFT JOIN products_silver ps
  ON d.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON d.store_id = ss.store_id
WHERE d.rn = 1
"""
transactions_silver_df = spark.sql(transactions_silver_sql)

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

job.commit()