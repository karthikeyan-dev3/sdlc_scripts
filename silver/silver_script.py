import sys
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

# -------------------------
# Read Source Tables (Bronze)
# -------------------------
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

# -------------------------
# Target: dim_product_silver
# -------------------------
dim_product_silver_df = spark.sql("""
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
  WHERE pb.product_id IS NOT NULL
) x
WHERE x.rn = 1
  AND x.is_active = TRUE
""")

(
    dim_product_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_product_silver.csv")
)

# -------------------------
# Target: dim_store_silver
# -------------------------
dim_store_silver_df = spark.sql("""
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
  WHERE sb.store_id IS NOT NULL
) x
WHERE x.rn = 1
""")

(
    dim_store_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_store_silver.csv")
)

# -------------------------
# Target: sales_txn_silver
# -------------------------
sales_txn_silver_df = spark.sql("""
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
    ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
) x
WHERE x.rn = 1
""")

(
    sales_txn_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_txn_silver.csv")
)

job.commit()