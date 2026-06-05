import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# 1) Read source tables (Bronze)
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

# ============================================================
# TABLE: silver.product_master_silver
# ============================================================
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(UPPER(pb.category)) AS product_category,
    CAST(pb.price AS double) AS price,
    TRIM(pb.brand) AS manufacturer
  FROM products_bronze pb
  WHERE pb.is_active = true
),
dedup AS (
  SELECT
    product_id,
    product_name,
    product_category,
    price,
    manufacturer,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY price DESC, product_name DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  product_category,
  price,
  manufacturer
FROM dedup
WHERE rn = 1
""")

product_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/product_master_silver.csv"
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# TABLE: silver.store_master_silver
# ============================================================
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
    CASE WHEN sb.state IS NOT NULL THEN sb.state END AS store_region
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_location,
    store_region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY sb_open_date DESC, store_name DESC
    ) AS rn
  FROM (
    SELECT
      base.store_id,
      base.store_name,
      base.store_location,
      base.store_region,
      open_date AS sb_open_date
    FROM base
    JOIN stores_bronze sb2
      ON base.store_id = sb2.store_id
     AND TRIM(sb2.store_name) = base.store_name
     AND CONCAT(TRIM(sb2.city), ', ', TRIM(sb2.state)) = base.store_location
     AND (CASE WHEN sb2.state IS NOT NULL THEN sb2.state END) <=> base.store_region
  ) x
)
SELECT
  store_id,
  store_name,
  store_location,
  store_region
FROM dedup
WHERE rn = 1
""")

store_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/store_master_silver.csv"
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# TABLE: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS date) AS transaction_date,
    stb.product_id AS product_id,
    stb.store_id AS store_id,
    CAST(stb.sale_amount AS double) AS sales_amount,
    CAST(stb.quantity AS int) AS quantity_sold
  FROM sales_transactions_bronze stb
  LEFT JOIN product_master_silver pms
    ON stb.product_id = pms.product_id
  LEFT JOIN store_master_silver sms
    ON stb.store_id = sms.store_id
  WHERE pms.product_id IS NOT NULL
    AND sms.store_id IS NOT NULL
    AND CAST(stb.quantity AS int) > 0
    AND CAST(stb.sale_amount AS double) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    sales_amount,
    quantity_sold,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_date DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  transaction_date,
  product_id,
  store_id,
  sales_amount,
  quantity_sold
FROM dedup
WHERE rn = 1
""")

sales_transactions_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)