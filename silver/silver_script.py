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

# ----------------------------
# 1) Read source tables (bronze)
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
# TABLE: silver.products_silver
# ============================================================
products_silver_sql = """
WITH ranked AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    CAST(pb.price AS double) AS price_raw,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY
        CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(pb.category) IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC,
        TRIM(pb.product_name) DESC,
        TRIM(pb.category) DESC,
        CAST(pb.price AS double) DESC
    ) AS rn
  FROM products_bronze pb
  WHERE COALESCE(CAST(pb.is_active AS boolean), false) = true
)
SELECT
  product_id,
  product_name,
  category,
  CAST(CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END AS double) AS price
FROM ranked
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

# ============================================================
# TABLE: silver.stores_silver
# ============================================================
stores_silver_sql = """
WITH ranked AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.state) AS region,
    TRIM(sb.store_name) AS store_manager,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY
        CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(sb.city) IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN TRIM(sb.state) IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END DESC,
        TRIM(sb.store_name) DESC,
        TRIM(sb.city) DESC,
        TRIM(sb.state) DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  region,
  store_manager
FROM ranked
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

# ============================================================
# TABLE: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH ranked AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(stb.transaction_time AS date) AS date,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY CAST(stb.transaction_time AS timestamp) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON TRIM(stb.product_id) = ps.product_id
  INNER JOIN stores_silver ss
    ON TRIM(stb.store_id) = ss.store_id
)
SELECT
  transaction_id,
  date,
  product_id,
  store_id,
  quantity_sold,
  total_sales_amount
FROM ranked
WHERE rn = 1
  AND quantity_sold > 0
  AND total_sales_amount >= 0
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

# ============================================================
# TABLE: silver.sales_aggregated_silver
# ============================================================
sales_aggregated_silver_sql = """
SELECT
  CAST(sts.date AS date) AS report_date,
  sts.product_id AS product_id,
  sts.store_id AS store_id,
  CAST(SUM(sts.quantity_sold) AS int) AS total_quantity_sold,
  CAST(SUM(sts.total_sales_amount) AS double) AS total_sales_amount,
  CAST(
    CASE
      WHEN SUM(sts.quantity_sold) = 0 THEN NULL
      ELSE SUM(sts.total_sales_amount) / SUM(sts.quantity_sold)
    END AS double
  ) AS average_price
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.date AS date),
  sts.product_id,
  sts.store_id
"""

sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()