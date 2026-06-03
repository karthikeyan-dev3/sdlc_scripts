import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# Read source tables from S3
# ----------------------------
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

# ----------------------------
# Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver.products_silver
# Source: bronze.products_bronze pb
# Output columns: product_id, product_name, category, brand
# ============================================================
products_silver_df = spark.sql(
    """
SELECT
  pb.product_id AS product_id,
  pb.product_name AS product_name,
  pb.category AS category,
  pb.brand AS brand
FROM products_bronze pb
"""
)

products_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/products_silver.csv"
)

products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# Target: silver.stores_silver
# Source: bronze.stores_bronze sb
# Output columns: store_id, store_name, location, region
# ============================================================
stores_silver_df = spark.sql(
    """
SELECT
  sb.store_id AS store_id,
  sb.store_name AS store_name,
  CONCAT(sb.city, ', ', sb.state) AS location,
  CASE sb.state WHEN sb.state THEN sb.state END AS region
FROM stores_bronze sb
"""
)

stores_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/stores_silver.csv"
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: silver.transactions_silver
# Source: bronze.sales_transactions_bronze stb
# Join: silver.products_silver ps, silver.stores_silver ss
# Output columns: transaction_id, store_id, product_id, sale_date, revenue, units_sold
# ============================================================
transactions_silver_df = spark.sql(
    """
SELECT
  stb.transaction_id AS transaction_id,
  stb.store_id AS store_id,
  stb.product_id AS product_id,
  DATE(stb.transaction_time) AS sale_date,
  stb.sale_amount AS revenue,
  stb.quantity AS units_sold
FROM sales_transactions_bronze stb
INNER JOIN products_silver ps
  ON stb.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON stb.store_id = ss.store_id
"""
)

transactions_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/transactions_silver.csv"
)

job.commit()
