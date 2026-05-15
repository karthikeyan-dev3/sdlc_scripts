import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables
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
# 3) Transform: product_master_silver (SQL)
# ----------------------------
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    pb.product_name AS product_name,
    pb.category AS category,
    pb.brand AS brand
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
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
  brand
FROM dedup
WHERE rn = 1
""")

# 4) Save output: product_master_silver
(
    product_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# Create temp view for downstream join
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ----------------------------
# 3) Transform: store_master_silver (SQL)
# ----------------------------
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    sb.store_name AS store_name,
    CONCAT(sb.city, ', ', sb.state) AS location,
    sb.state AS region,
    sb.store_type AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    region,
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
  region,
  store_type
FROM dedup
WHERE rn = 1
""")

# 4) Save output: store_master_silver
(
    store_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# Create temp view for downstream join
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ----------------------------
# 3) Transform: sales_transactions_silver (SQL)
# ----------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    stb.store_id AS store_id,
    stb.product_id AS product_id,
    stb.quantity AS quantity_sold,
    stb.sale_amount AS total_sales_amount,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  LEFT JOIN product_master_silver pms
    ON stb.product_id = pms.product_id
  LEFT JOIN store_master_silver sms
    ON stb.store_id = sms.store_id
),
filtered AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_sales_amount,
    transaction_time
  FROM base
  WHERE
    quantity_sold > 0
    AND total_sales_amount >= 0
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE rn = 1
""")

# 4) Save output: sales_transactions_silver
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