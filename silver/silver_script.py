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

# =========================
# Read Source Tables (Bronze)
# =========================

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =========================
# Target: silver.stores_silver
# =========================

stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    sb.store_name AS store_name,
    sb.city AS city,
    sb.state AS state,
    sb.store_type AS store_type,
    CAST(sb.open_date AS date) AS open_date
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    open_date,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY open_date DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  city,
  state,
  store_type,
  open_date
FROM dedup
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

# =========================
# Target: silver.products_silver
# =========================

products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    pb.product_name AS product_name,
    pb.category AS category,
    pb.brand AS brand,
    CAST(pb.price AS double) AS price,
    CAST(pb.is_active AS boolean) AS is_active
  FROM products_bronze pb
  WHERE COALESCE(CAST(pb.is_active AS boolean), false) = true
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active,
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
  brand,
  price,
  is_active
FROM dedup
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

# =========================
# Target: silver.sales_transactions_silver
# =========================

sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    stb.store_id AS store_id,
    stb.product_id AS product_id,
    CAST(stb.quantity AS int) AS quantity,
    CAST(stb.sale_amount AS double) AS sale_amount,
    CAST(stb.transaction_time AS timestamp) AS transaction_time,
    CAST(CAST(stb.transaction_time AS timestamp) AS date) AS reporting_date
  FROM sales_transactions_bronze stb
),
validated AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time,
    reporting_date
  FROM base
  WHERE COALESCE(quantity, 0) >= 0
    AND COALESCE(sale_amount, 0.0) >= 0.0
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time,
    reporting_date,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM validated
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time,
  reporting_date
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

# =========================
# Target: silver.sales_store_product_daily_silver
# =========================

sales_store_product_daily_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  sts.reporting_date AS reporting_date,
  SUM(sts.sale_amount) AS revenue,
  SUM(sts.quantity) AS quantity_sold
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.reporting_date
"""

sales_store_product_daily_silver_df = spark.sql(sales_store_product_daily_silver_sql)

(
    sales_store_product_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_store_product_daily_silver.csv")
)

job.commit()