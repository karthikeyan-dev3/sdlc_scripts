import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# TABLE: silver.product_details_silver
# ------------------------------------------------------------
product_details_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
      ORDER BY
        CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
)
SELECT
  product_id,
  product_name
FROM ranked
WHERE rn = 1
"""

product_details_silver_df = spark.sql(product_details_silver_sql)
product_details_silver_df.createOrReplaceTempView("product_details_silver")

(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_details_silver.csv")
)

# ------------------------------------------------------------
# TABLE: silver.store_details_silver
# ------------------------------------------------------------
store_details_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(sb.store_id) AS STRING)
      ORDER BY
        CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_name
FROM ranked
WHERE rn = 1
"""

store_details_silver_df = spark.sql(store_details_silver_sql)
store_details_silver_df.createOrReplaceTempView("store_details_silver")

(
    store_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_details_silver.csv")
)

# ------------------------------------------------------------
# TABLE: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH staged AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(COALESCE(CAST(stb.quantity AS INT), 0) AS INT) AS quantity_sold
  FROM sales_transactions_bronze stb
  LEFT JOIN product_details_silver pds
    ON CAST(TRIM(stb.product_id) AS STRING) = pds.product_id
  LEFT JOIN store_details_silver sds
    ON CAST(TRIM(stb.store_id) AS STRING) = sds.store_id
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND COALESCE(CAST(stb.quantity AS INT), 0) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    CAST(transaction_time AS DATE) AS sale_date,
    product_id,
    store_id,
    quantity_sold,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM staged
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  quantity_sold
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

# ------------------------------------------------------------
# TABLE: silver.daily_sales_summary_silver
# ------------------------------------------------------------
daily_sales_summary_silver_sql = """
SELECT
  CAST(stb.transaction_time AS DATE) AS aggregation_date,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  SUM(COALESCE(CAST(stb.sale_amount AS DOUBLE), 0D)) AS daily_total_revenue,
  COUNT(DISTINCT sts.transaction_id) AS daily_transaction_count
FROM sales_transactions_silver sts
INNER JOIN sales_transactions_bronze stb
  ON sts.transaction_id = CAST(TRIM(stb.transaction_id) AS STRING)
GROUP BY
  CAST(stb.transaction_time AS DATE),
  sts.store_id,
  sts.product_id
"""

daily_sales_summary_silver_df = spark.sql(daily_sales_summary_silver_sql)
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

(
    daily_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_summary_silver.csv")
)