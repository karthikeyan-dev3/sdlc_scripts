import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables (Bronze)
# ----------------------------
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

# ----------------------------
# 2) product_silver
# ----------------------------
product_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(pb.price AS DOUBLE) AS price
  FROM products_bronze pb
),
ranked AS (
  SELECT
    product_id,
    product_name,
    category,
    CAST(price AS DOUBLE) AS price,
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
  price
FROM ranked
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ----------------------------
# 3) store_silver
# ----------------------------
store_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location
  FROM stores_bronze sb
),
ranked AS (
  SELECT
    store_id,
    store_name,
    location,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  location
FROM ranked
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ----------------------------
# 4) sales_transactions_silver
# ----------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sales_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS revenue
  FROM sales_transactions_bronze stb
  INNER JOIN product_silver ps
    ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
  INNER JOIN store_silver ss
    ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
),
filtered AS (
  SELECT *
  FROM joined
  WHERE COALESCE(quantity_sold, 0) >= 0
    AND COALESCE(revenue, 0D) >= 0D
),
ranked AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sales_date,
    quantity_sold,
    revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_id
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sales_date,
  quantity_sold,
  revenue
FROM ranked
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ----------------------------
# 5) aggregated_sales_silver
# ----------------------------
aggregated_sales_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.sales_date AS sales_date,
  SUM(sts.revenue) AS total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions,
  SUM(sts.quantity_sold) AS total_quantity
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.sales_date
"""
aggregated_sales_silver_df = spark.sql(aggregated_sales_silver_sql)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
