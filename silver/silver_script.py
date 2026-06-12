import sys
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

# -----------------------------
# Read source tables from S3
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------
# products_silver
# -----------------------------
products_silver_sql = """
SELECT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(brand) AS brand,
  CAST(price AS DOUBLE) AS price,
  COALESCE(CAST(is_active AS BOOLEAN), TRUE) AS is_active
FROM (
  SELECT
    pb.product_id,
    pb.product_name,
    pb.category,
    pb.brand,
    pb.price,
    pb.is_active,
    ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
  FROM products_bronze pb
) d
WHERE rn = 1
  AND product_id IS NOT NULL
"""
products_silver_df = spark.sql(products_silver_sql)

products_silver_output_path = f"{TARGET_PATH}/products_silver.csv"
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(products_silver_output_path)
)

# -----------------------------
# stores_silver
# -----------------------------
stores_silver_sql = """
SELECT
  store_id,
  TRIM(store_name) AS store_name,
  TRIM(city) AS city,
  TRIM(state) AS state,
  TRIM(store_type) AS store_type,
  CAST(open_date AS DATE) AS open_date
FROM (
  SELECT
    sb.store_id,
    sb.store_name,
    sb.city,
    sb.state,
    sb.store_type,
    sb.open_date,
    ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
  FROM stores_bronze sb
) d
WHERE rn = 1
  AND store_id IS NOT NULL
"""
stores_silver_df = spark.sql(stores_silver_sql)

stores_silver_output_path = f"{TARGET_PATH}/stores_silver.csv"
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(stores_silver_output_path)
)

# -----------------------------
# sales_transactions_silver
# -----------------------------
sales_transactions_silver_sql = """
SELECT
  transaction_id,
  store_id,
  product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(sale_amount AS DOUBLE) AS sale_amount,
  CAST(transaction_time AS TIMESTAMP) AS transaction_time
FROM (
  SELECT
    stb.transaction_id,
    stb.store_id,
    stb.product_id,
    stb.quantity,
    stb.sale_amount,
    stb.transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id, stb.store_id, stb.product_id, stb.transaction_time
      ORDER BY stb.transaction_id
    ) AS rn
  FROM sales_transactions_bronze stb
) d
WHERE rn = 1
  AND transaction_id IS NOT NULL
  AND store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND transaction_time IS NOT NULL
  AND quantity IS NOT NULL
  AND quantity > 0
  AND sale_amount IS NOT NULL
  AND sale_amount >= 0
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

sales_transactions_silver_output_path = f"{TARGET_PATH}/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(sales_transactions_silver_output_path)
)

job.commit()