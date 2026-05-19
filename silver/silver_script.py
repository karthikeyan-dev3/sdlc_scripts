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

# -----------------------------
# 1) Read source tables (Bronze)
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
# 2) Create temp views
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver.product_master_silver
# Source: bronze.products_bronze pb
# ============================================================
product_master_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    pb.product_name AS product_name,
    pb.category AS category,
    pb.brand AS brand,
    CAST(pb.price AS FLOAT) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 1 ELSE 0 END +
        CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM base
WHERE rn = 1
  AND price >= 0
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ============================================================
# Target: silver.store_master_silver
# Source: bronze.stores_bronze sb
# ============================================================
store_master_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    sb.store_name AS store_name,
    CONCAT(sb.city, ', ', sb.state) AS location,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE NULL
    END AS region,
    sb.store_type AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END +
        CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END +
        CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END +
        CASE WHEN sb.store_type IS NOT NULL AND TRIM(sb.store_type) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  location,
  region,
  store_type
FROM base
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ============================================================
# Target: silver.sales_transactions_silver
# Source: bronze.sales_transactions_bronze stb
#         INNER JOIN silver.product_master_silver pms
#         INNER JOIN silver.store_master_silver sms
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    pms.product_id AS product_id,
    sms.store_id AS store_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY
        CASE WHEN stb.transaction_time IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN stb.quantity IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN stb.sale_amount IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON stb.product_id = pms.product_id
  INNER JOIN store_master_silver sms
    ON stb.store_id = sms.store_id
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  quantity_sold,
  total_sales_amount
FROM base
WHERE rn = 1
  AND quantity_sold > 0
  AND total_sales_amount >= 0
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()