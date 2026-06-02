import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables
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
# 2) Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(pb.product_id)) AS product_id,
    TRIM(pb.product_name)      AS product_name,
    TRIM(pb.category)          AS category,
    TRIM(pb.brand)             AS brand,
    CAST(pb.price AS DECIMAL(18,2)) AS price
  FROM products_bronze pb
  WHERE
    TRIM(COALESCE(pb.product_id, '')) <> ''
    AND LOWER(TRIM(COALESCE(pb.is_active, 'false'))) = 'true'
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY price DESC, product_name DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM dedup
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# ============================================================
# Target: stores_silver
# ============================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(sb.store_id)) AS store_id,
    TRIM(sb.store_name)      AS store_name,
    CAST(sb.open_date AS DATE) AS store_open_date,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','NJ','NY','PA','RI','VT') THEN 'Northeast'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
      WHEN UPPER(TRIM(sb.state)) IN ('AK','AZ','CA','CO','HI','ID','MT','NV','NM','OR','UT','WA','WY') THEN 'West'
      ELSE NULL
    END AS region
  FROM stores_bronze sb
  WHERE TRIM(COALESCE(sb.store_id, '')) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    store_open_date,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_open_date DESC, store_name DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region,
  store_open_date
FROM dedup
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# Target: sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(stb.transaction_id)) AS transaction_id,
    TRIM(UPPER(stb.store_id))       AS store_id,
    TRIM(UPPER(stb.product_id))     AS product_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(stb.quantity AS INT)       AS quantity_sold,
    CAST(stb.sale_amount AS DECIMAL(18,2)) AS total_amount,
    stb.transaction_time            AS transaction_time
  FROM sales_transactions_bronze stb
  INNER JOIN stores_silver ss
    ON TRIM(UPPER(stb.store_id)) = ss.store_id
  INNER JOIN products_silver ps
    ON TRIM(UPPER(stb.product_id)) = ps.product_id
  WHERE
    TRIM(COALESCE(stb.transaction_id, '')) <> ''
    AND TRIM(COALESCE(stb.store_id, '')) <> ''
    AND TRIM(COALESCE(stb.product_id, '')) <> ''
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DECIMAL(18,2)) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_date,
  quantity_sold,
  total_amount
FROM dedup
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)