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

# =============================================================================
# 1) Read source tables (Bronze)
# =============================================================================
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

# =============================================================================
# 2) products_silver
#    - Cleanses text fields (TRIM/UPPER where applicable)
#    - Filters to active products (is_active = true)
#    - De-duplicates by product_id keeping "latest/most complete" record
#      (no audit columns in UDT; use completeness heuristic on key attributes)
# =============================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    UPPER(TRIM(pb.category)) AS category,
    UPPER(TRIM(pb.brand)) AS brand
  FROM products_bronze pb
  WHERE pb.is_active = true
),
ranked AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN brand IS NOT NULL AND brand <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand
FROM ranked
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =============================================================================
# 3) stores_silver
#    - Standardizes location fields (TRIM/UPPER)
#    - Derives store_location as 'city, state'
#    - Sets store_manager to NULL
#    - De-duplicates by store_id keeping "latest/most complete" record
# =============================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    (UPPER(TRIM(sb.city)) || ', ' || UPPER(TRIM(sb.state))) AS store_location,
    CAST(NULL AS STRING) AS store_manager
  FROM stores_bronze sb
),
ranked AS (
  SELECT
    store_id,
    store_location,
    store_manager,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_location IS NOT NULL AND store_location <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_location,
  store_manager
FROM ranked
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =============================================================================
# 4) sales_transactions_silver
#    - Enforces referential integrity (join to products_silver + stores_silver)
#    - transaction_date = DATE(transaction_time)
#    - sales_amount = sale_amount; units_sold = quantity
#    - De-duplicates by transaction_id keeping latest transaction_time
# =============================================================================
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    DATE(stb.transaction_time) AS transaction_date,
    CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
    CAST(stb.quantity AS INT) AS units_sold,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON TRIM(stb.product_id) = ps.product_id
  INNER JOIN stores_silver ss
    ON TRIM(stb.store_id) = ss.store_id
),
ranked AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    sales_amount,
    units_sold,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM joined
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  sales_amount,
  units_sold
FROM ranked
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =============================================================================
# 5) cleaned_sales_silver
#    - De-duplicates by transaction_id keeping latest transaction_time
#    - Standardizes IDs (TRIM/UPPER)
#    - cleaned_transaction_date = DATE(transaction_time)
#    - Enforces non-negative cleaned_sales_amount and non-negative units
#    - Outputs: transaction_id, cleaned_product_id, cleaned_store_id,
#               cleaned_transaction_date, cleaned_sales_amount
# =============================================================================
cleaned_sales_silver_sql = """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    UPPER(TRIM(stb.product_id)) AS cleaned_product_id,
    UPPER(TRIM(stb.store_id)) AS cleaned_store_id,
    DATE(stb.transaction_time) AS cleaned_transaction_date,
    CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(stb.quantity AS INT) AS quantity,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
),
filtered AS (
  SELECT
    transaction_id,
    cleaned_product_id,
    cleaned_store_id,
    cleaned_transaction_date,
    sale_amount,
    quantity,
    transaction_time
  FROM base
  WHERE CAST(sale_amount AS DOUBLE) >= 0
    AND CAST(quantity AS INT) >= 0
),
ranked AS (
  SELECT
    transaction_id,
    cleaned_product_id,
    cleaned_store_id,
    cleaned_transaction_date,
    sale_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  cleaned_product_id,
  cleaned_store_id,
  cleaned_transaction_date,
  sale_amount AS cleaned_sales_amount
FROM ranked
WHERE rn = 1
"""
cleaned_sales_silver_df = spark.sql(cleaned_sales_silver_sql)
cleaned_sales_silver_df.createOrReplaceTempView("cleaned_sales_silver")

(
    cleaned_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/cleaned_sales_silver.csv")
)

# =============================================================================
# 6) sales_aggregated_silver
#    - Aggregates from sales_transactions_silver
#    - Group by store_id, product_id, record_date = transaction_date
# =============================================================================
sales_aggregated_silver_sql = """
SELECT
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  SUM(sts.sales_amount) AS total_sales_amount,
  SUM(sts.units_sold) AS total_units_sold,
  AVG(sts.sales_amount) AS average_sales_amount,
  sts.transaction_date AS record_date
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id,
  sts.product_id,
  sts.transaction_date
"""
sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()