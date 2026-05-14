import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 + create temp views (bronze)
# ------------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------------
# 2) product_master_silver (silver.product_master_silver)
# ------------------------------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS float) AS price
  FROM products_bronze pb
  WHERE pb.is_active = true
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
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
product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------------
# 3) store_master_silver (silver.store_master_silver)
# ------------------------------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_location,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_location
FROM dedup
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

# ------------------------------------------------------------------------------------
# 4) sales_transactions_silver (silver.sales_transactions_silver)
# ------------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(stb.transaction_time AS date) AS sale_date,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_revenue
  FROM sales_transactions_bronze stb
  INNER JOIN product_master_silver pms
    ON TRIM(stb.product_id) = pms.product_id
  INNER JOIN store_master_silver sms
    ON TRIM(stb.store_id) = sms.store_id
  WHERE CAST(stb.quantity AS int) > 0
    AND CAST(stb.sale_amount AS double) > 0
),
dedup AS (
  SELECT
    transaction_id,
    sale_date,
    product_id,
    store_id,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM joined
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  quantity_sold,
  total_revenue
FROM dedup
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

# ------------------------------------------------------------------------------------
# 5) aggregated_sales_silver (silver.aggregated_sales_silver)
# ------------------------------------------------------------------------------------
aggregated_sales_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  sts.sale_date AS aggregation_date,
  SUM(sts.quantity_sold) AS total_quantity_sold,
  SUM(sts.total_revenue) AS total_sales_revenue
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.sale_date
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