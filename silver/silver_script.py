import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
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
# TABLE: silver.products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_id) DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand
FROM base
WHERE rn = 1
  AND product_id IS NOT NULL
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
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.state) AS STRING) AS region,
    CAST(TRIM(sb.store_type) AS STRING) AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY TRIM(sb.store_id) DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  region,
  store_type
FROM base
WHERE rn = 1
  AND store_id IS NOT NULL
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
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(ps.product_id) AS STRING) AS product_id,
    CAST(TRIM(ss.store_id) AS STRING) AS store_id,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON TRIM(stb.product_id) = TRIM(ps.product_id)
  INNER JOIN stores_silver ss
    ON TRIM(stb.store_id) = TRIM(ss.store_id)
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  quantity_sold,
  total_amount
FROM base
WHERE rn = 1
  AND transaction_id IS NOT NULL
  AND quantity_sold IS NOT NULL AND quantity_sold >= 0
  AND total_amount IS NOT NULL AND total_amount >= 0
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
# TABLE: silver.aggregated_sales_silver
# ============================================================
aggregated_sales_silver_sql = """
SELECT
  CAST(sts.transaction_date AS DATE) AS date,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.transaction_date AS DATE),
  CAST(sts.product_id AS STRING),
  CAST(sts.store_id AS STRING)
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

# ============================================================
# TABLE: silver.sales_analysis_silver
# ============================================================
sales_analysis_silver_sql = """
WITH per_date AS (
  SELECT
    CAST(sas.date AS DATE) AS analysis_date,
    CAST(SUM(CAST(sas.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_store_sales,
    CAST(AVG(CAST(sas.total_sales_amount AS DOUBLE)) AS DOUBLE) AS average_product_sales
  FROM aggregated_sales_silver sas
  GROUP BY CAST(sas.date AS DATE)
),
top_product AS (
  SELECT
    CAST(sas.date AS DATE) AS analysis_date,
    CAST(sas.product_id AS STRING) AS top_selling_product_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sas.date AS DATE)
      ORDER BY CAST(sas.total_sales_amount AS DOUBLE) DESC, CAST(sas.product_id AS STRING) ASC
    ) AS rn
  FROM aggregated_sales_silver sas
),
top_store AS (
  SELECT
    CAST(sas.date AS DATE) AS analysis_date,
    CAST(sas.store_id AS STRING) AS top_selling_store_id,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sas.date AS DATE)
      ORDER BY CAST(sas.total_sales_amount AS DOUBLE) DESC, CAST(sas.store_id AS STRING) ASC
    ) AS rn
  FROM aggregated_sales_silver sas
)
SELECT
  d.analysis_date,
  d.total_store_sales,
  d.average_product_sales,
  p.top_selling_product_id,
  s.top_selling_store_id
FROM per_date d
INNER JOIN top_product p
  ON d.analysis_date = p.analysis_date AND p.rn = 1
INNER JOIN top_store s
  ON d.analysis_date = s.analysis_date AND s.rn = 1
"""

sales_analysis_silver_df = spark.sql(sales_analysis_silver_sql)

(
    sales_analysis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_analysis_silver.csv")
)

job.commit()