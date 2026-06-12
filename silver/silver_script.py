import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------
# 1) Read source tables from S3
# -------------------------------
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

# -------------------------------
# 2) Create temp views
# -------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# TABLE: products_silver
# ============================================================
products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    CASE WHEN TRIM(pb.product_name) = '' THEN NULL ELSE TRIM(pb.product_name) END AS product_name,
    CASE WHEN TRIM(pb.category) = '' THEN NULL ELSE TRIM(pb.category) END AS category,
    CASE WHEN TRIM(pb.brand) = '' THEN NULL ELSE TRIM(pb.brand) END AS brand,
    CASE
      WHEN CAST(pb.price AS double) >= 0 THEN CAST(pb.price AS double)
      ELSE NULL
    END AS price
  FROM products_bronze pb
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
  CAST(price AS float) AS price
FROM dedup
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

products_silver_output_path = TARGET_PATH + "/products_silver.csv"
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(products_silver_output_path)
)

# ============================================================
# TABLE: stores_silver
# ============================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    CASE WHEN TRIM(sb.store_name) = '' THEN NULL ELSE TRIM(sb.store_name) END AS store_name,
    CONCAT(
      CASE WHEN TRIM(sb.city) = '' THEN NULL ELSE TRIM(sb.city) END,
      ', ',
      CASE WHEN TRIM(sb.state) = '' THEN NULL ELSE TRIM(sb.state) END
    ) AS location,
    CASE WHEN TRIM(sb.store_type) = '' THEN NULL ELSE TRIM(sb.store_type) END AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    store_type,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  location,
  store_type
FROM dedup
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

stores_silver_output_path = TARGET_PATH + "/stores_silver.csv"
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(stores_silver_output_path)
)

# ============================================================
# TABLE: sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS date) AS transaction_date,
    ss.store_id AS store_id,
    ps.product_id AS product_id,
    CASE
      WHEN CAST(stb.quantity AS int) > 0 THEN CAST(stb.quantity AS int)
      ELSE NULL
    END AS quantity,
    CASE
      WHEN COALESCE(CAST(stb.sale_amount AS double), CAST(stb.quantity AS double) * CAST(ps.price AS double)) >= 0
        THEN COALESCE(CAST(stb.sale_amount AS double), CAST(stb.quantity AS double) * CAST(ps.price AS double))
      ELSE NULL
    END AS total_revenue
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON stb.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON stb.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
  FROM joined
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity,
  total_revenue
FROM dedup
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

sales_transactions_silver_output_path = TARGET_PATH + "/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_transactions_silver_output_path)
)

# ============================================================
# TABLE: sales_aggregated_silver
# ============================================================
sales_aggregated_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  sts.transaction_date AS aggregated_date,
  SUM(sts.quantity) AS total_quantity,
  SUM(sts.total_revenue) AS total_revenue,
  CASE
    WHEN SUM(sts.quantity) > 0 THEN SUM(sts.total_revenue) / SUM(sts.quantity)
    ELSE NULL
  END AS average_price
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.transaction_date
"""

sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

sales_aggregated_silver_output_path = TARGET_PATH + "/sales_aggregated_silver.csv"
(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_aggregated_silver_output_path)
)

job.commit()
