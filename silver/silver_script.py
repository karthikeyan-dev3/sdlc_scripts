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

# -------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# -------------------------------------------------------------------

products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------------------------------------------------
# Target: products_silver
# -------------------------------------------------------------------

products_silver_df = spark.sql("""
SELECT
  TRIM(pb.product_id) AS product_id,
  TRIM(pb.product_name) AS product_name,
  TRIM(pb.category) AS category,
  TRIM(pb.brand) AS brand,
  CAST(pb.price AS DOUBLE) AS price,
  CAST(pb.is_active AS BOOLEAN) AS is_active
FROM (
  SELECT
    pb.*,
    ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
) pb
WHERE pb.rn = 1
  AND COALESCE(pb.is_active, TRUE) = TRUE
""")

(
    products_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -------------------------------------------------------------------
# Target: stores_silver
# -------------------------------------------------------------------

stores_silver_df = spark.sql("""
SELECT
  TRIM(sb.store_id) AS store_id,
  TRIM(sb.store_name) AS store_name,
  TRIM(sb.city) AS city,
  TRIM(sb.state) AS state,
  TRIM(sb.store_type) AS store_type,
  CAST(sb.open_date AS DATE) AS open_date
FROM (
  SELECT
    sb.*,
    ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
) sb
WHERE sb.rn = 1
""")

(
    stores_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -------------------------------------------------------------------
# Target: sales_transactions_silver
# -------------------------------------------------------------------

sales_transactions_silver_df = spark.sql("""
SELECT
  TRIM(stb.transaction_id) AS transaction_id,
  TRIM(stb.store_id) AS store_id,
  TRIM(stb.product_id) AS product_id,
  CAST(stb.quantity AS INT) AS quantity,
  CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
  CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
FROM (
  SELECT
    stb.*,
    ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
) stb
WHERE stb.rn = 1
  AND COALESCE(stb.quantity, 0) >= 0
  AND COALESCE(stb.sale_amount, 0.0) >= 0.0
""")

(
    sales_transactions_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()