import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------------------------------------------------
# 3) Transformations using Spark SQL (with ROW_NUMBER for dedup)
# -------------------------------------------------------------------

# products_silver
products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING)      AS product_id,
    CAST(pb.product_name AS STRING)    AS product_name,
    CAST(pb.category AS STRING)        AS category,
    CAST(pb.brand AS STRING)           AS brand,
    CAST(pb.price AS FLOAT)            AS price,
    CAST(pb.is_active AS BOOLEAN)      AS is_active
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM dedup
WHERE rn = 1
""")

# stores_silver
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING)      AS store_id,
    CAST(sb.store_name AS STRING)    AS store_name,
    CAST(sb.city AS STRING)          AS city,
    CAST(sb.state AS STRING)         AS state,
    CAST(sb.store_type AS STRING)    AS store_type,
    DATE(CAST(sb.open_date AS STRING)) AS open_date
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    open_date,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM dedup
WHERE rn = 1
""")

# sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING)   AS transaction_id,
    CAST(stb.store_id AS STRING)         AS store_id,
    CAST(stb.product_id AS STRING)       AS product_id,
    CAST(stb.quantity AS INT)            AS quantity,
    CAST(stb.sale_amount AS DOUBLE)      AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
  FROM sales_transactions_bronze stb
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM base
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time
FROM dedup
WHERE rn = 1
""")

# -------------------------------------------------------------------
# 4) Save output (single CSV file directly under TARGET_PATH)
# -------------------------------------------------------------------
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()