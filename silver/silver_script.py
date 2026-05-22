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
# 1) Read source tables (Bronze)
# ----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
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

# ----------------------------
# 2) Create temp views
# ----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# Target: sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH dedup AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS date) AS sale_date,
    stb.store_id AS store_id,
    stb.product_id AS product_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
)
SELECT
  transaction_id,
  sale_date,
  store_id,
  product_id,
  quantity_sold,
  total_amount
FROM dedup
WHERE rn = 1
  AND transaction_id IS NOT NULL
  AND store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity_sold > 0
  AND total_amount >= 0
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: product_master_silver
# ============================================================
product_master_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS double) AS price,
    TO_JSON(MAP('is_active', pb.is_active)) AS attributes,
    pb.is_active AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    attributes,
    is_active
  FROM base
  WHERE rn = 1
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  attributes
FROM dedup
WHERE is_active = true
  AND price >= 0
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# Target: store_master_silver
# ============================================================
store_master_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(sb.store_name) AS store_name,
    sb.state AS region,
    TRIM(sb.city) AS city,
    sb.store_type AS store_area,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.store_id
    ) AS rn
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    city,
    store_area
  FROM base
  WHERE rn = 1
)
SELECT
  store_id,
  store_name,
  region,
  city,
  store_area
FROM dedup
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# Target: aggregated_sales_silver
# ============================================================
aggregated_sales_silver_sql = """
SELECT
  sts.sale_date AS report_date,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  SUM(sts.quantity_sold) AS total_quantity_sold,
  SUM(sts.total_amount) AS total_sales_amount,
  CASE
    WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.total_amount) / SUM(sts.quantity_sold)
    ELSE NULL
  END AS average_price
FROM sales_transactions_silver sts
GROUP BY
  sts.sale_date,
  sts.store_id,
  sts.product_id
"""

aggregated_sales_silver_df = spark.sql(aggregated_sales_silver_sql)
(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ============================================================
# Target: sales_performance_silver
# ============================================================
sales_performance_silver_sql = """
WITH monthly AS (
  SELECT
    ass.store_id AS store_id,
    ass.product_id AS product_id,
    DATE_FORMAT(ass.report_date, 'yyyy-MM') AS sale_period,
    SUM(ass.total_sales_amount) AS current_period_sales
  FROM aggregated_sales_silver ass
  GROUP BY
    ass.store_id,
    ass.product_id,
    DATE_FORMAT(ass.report_date, 'yyyy-MM')
),
calc AS (
  SELECT
    store_id,
    product_id,
    sale_period,
    current_period_sales,
    LAG(current_period_sales) OVER (
      PARTITION BY store_id, product_id
      ORDER BY sale_period
    ) AS prior_period_sales
  FROM monthly
)
SELECT
  store_id,
  product_id,
  sale_period,
  (current_period_sales - prior_period_sales) / NULLIF(prior_period_sales, 0) AS sales_growth_rate,
  0.0 AS average_discount
FROM calc
"""

sales_performance_silver_df = spark.sql(sales_performance_silver_sql)
(
    sales_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_performance_silver.csv")
)