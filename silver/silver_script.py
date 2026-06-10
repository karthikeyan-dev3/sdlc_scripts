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

# ----------------------------
# 1) Read source tables from S3
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
# 2) Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------
# 3) Spark SQL transformations per target table
# ----------------------------

# products_silver
products_silver_sql = """
SELECT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(brand) AS brand,
  CAST(price AS DOUBLE) AS price,
  COALESCE(is_active, TRUE) AS is_active
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
WHERE rn = 1
  AND is_active = TRUE
"""
products_silver_df = spark.sql(products_silver_sql)

# stores_silver
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
  WHERE sb.store_id IS NOT NULL
) x
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)

# sales_transactions_silver
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
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    stb.quantity,
    stb.sale_amount,
    stb.transaction_time,
    ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
  FROM sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
    AND stb.quantity IS NOT NULL
    AND stb.quantity > 0
    AND stb.sale_amount IS NOT NULL
    AND stb.sale_amount >= 0
    AND stb.transaction_time IS NOT NULL
) x
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

# ----------------------------
# 4) Write each target table separately as SINGLE CSV file directly under TARGET_PATH
# ----------------------------
(
    products_silver_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

(
    stores_silver_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

(
    sales_transactions_silver_df.coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
