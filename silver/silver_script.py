from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue / Spark setup
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read source tables from S3 (Bronze)
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# Target: silver.products_silver (ps)
# -----------------------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING)  AS product_id,
    CAST(pb.product_name AS STRING) AS product_name,
    CAST(pb.category AS STRING)     AS category,
    CAST(pb.brand AS STRING)        AS brand,
    CAST(pb.price AS FLOAT)         AS price,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(pb.product_id AS STRING)
      ORDER BY CAST(pb.product_id AS STRING)
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
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.stores_silver (ss)
# -----------------------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING)    AS store_id,
    CAST(sb.store_name AS STRING)  AS store_name,
    CAST(sb.city AS STRING)        AS city,
    CAST(sb.state AS STRING)       AS state,
    CAST(sb.store_type AS STRING)  AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sb.store_id AS STRING)
      ORDER BY CAST(sb.store_id AS STRING)
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type
FROM base
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_transactions_silver (sts)
# -----------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales
  FROM sales_transactions_bronze stb
  LEFT JOIN products_silver ps
    ON CAST(stb.product_id AS STRING) = ps.product_id
  LEFT JOIN stores_silver ss
    ON CAST(stb.store_id AS STRING) = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_id
    ) AS rn
  FROM joined
)
SELECT
  transaction_id,
  transaction_date,
  product_id,
  store_id,
  quantity_sold,
  total_sales
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)