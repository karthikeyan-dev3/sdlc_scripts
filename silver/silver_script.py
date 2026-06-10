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

# -----------------------------
# Read source tables from S3
# -----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------
# stores_silver
# -----------------------------
stores_silver_df = spark.sql("""
SELECT DISTINCT
  TRIM(sb.store_id) AS store_id,
  NULLIF(TRIM(sb.store_name),'') AS store_name,
  NULLIF(TRIM(sb.store_type),'') AS store_type,
  NULLIF(TRIM(sb.city),'') AS city,
  NULLIF(TRIM(sb.state),'') AS state,
  CAST(sb.open_date AS date) AS open_date
FROM stores_bronze sb
WHERE sb.store_id IS NOT NULL
""")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------
# products_silver
# -----------------------------
products_silver_df = spark.sql("""
SELECT DISTINCT
  TRIM(pb.product_id) AS product_id,
  NULLIF(TRIM(pb.product_name),'') AS product_name,
  NULLIF(TRIM(pb.brand),'') AS brand,
  NULLIF(TRIM(pb.category),'') AS category,
  CAST(pb.price AS double) AS price,
  COALESCE(CAST(pb.is_active AS boolean), true) AS is_active
FROM products_bronze pb
WHERE pb.product_id IS NOT NULL
""")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------
# sales_transactions_silver
# -----------------------------
sales_transactions_silver_df = spark.sql("""
SELECT DISTINCT
  TRIM(stb.transaction_id) AS transaction_id,
  NULLIF(TRIM(stb.store_id),'') AS store_id,
  NULLIF(TRIM(stb.product_id),'') AS product_id,
  CAST(stb.quantity AS int) AS quantity,
  CAST(stb.sale_amount AS double) AS sale_amount,
  CAST(stb.transaction_time AS timestamp) AS transaction_time
FROM sales_transactions_bronze stb
WHERE stb.transaction_id IS NOT NULL
  AND stb.transaction_time IS NOT NULL
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
