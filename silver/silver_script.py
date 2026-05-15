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

# -----------------------------
# 2) Create temp views (Bronze)
# -----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand
  FROM base
  WHERE rn = 1
)
SELECT
  CAST(product_id AS STRING) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(category AS STRING) AS category,
  CAST(brand AS STRING) AS brand
FROM dedup
""")

products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ============================================================
# Target: silver.stores_silver
# ============================================================
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE TRIM(sb.state)
    END AS region,
    TRIM(sb.store_type) AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.store_id
    ) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    store_type
  FROM base
  WHERE rn = 1
)
SELECT
  CAST(store_id AS STRING) AS store_id,
  CAST(store_name AS STRING) AS store_name,
  CAST(region AS STRING) AS region,
  CAST(store_type AS STRING) AS store_type
FROM dedup
""")

stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    CAST(ps.product_id AS STRING) AS product_id,
    CAST(ss.store_id AS STRING) AS store_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON stb.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON stb.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    quantity_sold,
    sales_amount
  FROM base
  WHERE rn = 1
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  quantity_sold,
  sales_amount
FROM dedup
WHERE quantity_sold > 0
  AND sales_amount > 0
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