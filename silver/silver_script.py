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

# ------------------------------------------------------------------------------
# 1) Read source tables (Bronze) + Create temp views
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 2) products_silver: cleanse + filter active + enforce non-null key + dedup
# ------------------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    CAST(pb.price AS DOUBLE) AS price,
    pb.is_active AS is_active
  FROM products_bronze pb
),
filtered AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    CAST(price AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_id
    ) AS rn
  FROM base
  WHERE product_id IS NOT NULL
    AND TRIM(product_id) <> ''
    AND COALESCE(LOWER(TRIM(CAST(is_active AS STRING))), '') IN ('true', '1', 'y', 'yes')
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  CAST(price AS DOUBLE) AS price
FROM filtered
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(TARGET_PATH + "/products_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) stores_silver: standardize name/city/state, derive location + region,
#    enforce non-null key + dedup
# ------------------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(TRIM(sb.state) AS STRING) AS state
  FROM stores_bronze sb
),
enriched AS (
  SELECT
    store_id,
    store_name,
    CONCAT(COALESCE(city, ''), ', ', COALESCE(state, '')) AS location,
    CASE
      WHEN UPPER(state) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN UPPER(state) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN UPPER(state) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN UPPER(state) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE NULL
    END AS region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_id
    ) AS rn
  FROM base
  WHERE store_id IS NOT NULL
    AND TRIM(store_id) <> ''
)
SELECT
  store_id,
  store_name,
  location,
  region
FROM enriched
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(TARGET_PATH + "/stores_silver.csv")
)

# ------------------------------------------------------------------------------
# 4) sales_transactions_silver: validate + conform + dedup + joins to product/store
# ------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount
  FROM sales_transactions_bronze stb
),
validated AS (
  SELECT
    transaction_id,
    CAST(transaction_time AS DATE) AS transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
  WHERE transaction_id IS NOT NULL
    AND TRIM(transaction_id) <> ''
    AND quantity_sold > 0
    AND total_sales_amount >= 0
)
SELECT
  v.transaction_id,
  v.transaction_date,
  v.product_id,
  v.store_id,
  v.quantity_sold,
  v.total_sales_amount
FROM validated v
INNER JOIN products_silver ps
  ON v.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON v.store_id = ss.store_id
WHERE v.rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(TARGET_PATH + "/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------
# 5) sales_aggregates_silver: daily store-product aggregation
# ------------------------------------------------------------------------------
sales_aggregates_silver_sql = """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
  CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS total_revenue,
  CAST(sts.transaction_date AS DATE) AS transaction_date
FROM sales_transactions_silver sts
GROUP BY
  sts.transaction_date,
  sts.store_id,
  sts.product_id
"""
sales_aggregates_silver_df = spark.sql(sales_aggregates_silver_sql)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(TARGET_PATH + "/sales_aggregates_silver.csv")
)

# ------------------------------------------------------------------------------
# 6) data_quality_silver: centralized record-level validation outcomes
# ------------------------------------------------------------------------------
data_quality_silver_sql = """
WITH sales_dq AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS record_id,
    CAST('sales_transactions_bronze' AS STRING) AS data_source,
    CASE
      WHEN stb.transaction_id IS NULL OR TRIM(CAST(stb.transaction_id AS STRING)) = '' THEN 'INVALID_MISSING_TRANSACTION_ID'
      WHEN stb.product_id IS NULL OR TRIM(CAST(stb.product_id AS STRING)) = '' THEN 'INVALID_MISSING_PRODUCT_ID'
      WHEN stb.store_id IS NULL OR TRIM(CAST(stb.store_id AS STRING)) = '' THEN 'INVALID_MISSING_STORE_ID'
      WHEN CAST(stb.quantity AS INT) IS NULL OR CAST(stb.quantity AS INT) <= 0 THEN 'INVALID_QUANTITY'
      WHEN CAST(stb.sale_amount AS DOUBLE) IS NULL OR CAST(stb.sale_amount AS DOUBLE) < 0 THEN 'INVALID_AMOUNT'
      WHEN ps.product_id IS NULL THEN 'INVALID_PRODUCT_FK'
      WHEN ss.store_id IS NULL THEN 'INVALID_STORE_FK'
      WHEN dup.rn > 1 THEN 'DUPLICATE_TRANSACTION_ID'
      ELSE 'VALID'
    END AS validation_status,
    CAST(stb.transaction_time AS DATE) AS validation_date
  FROM sales_transactions_bronze stb
  LEFT JOIN products_silver ps
    ON CAST(stb.product_id AS STRING) = ps.product_id
  LEFT JOIN stores_silver ss
    ON CAST(stb.store_id AS STRING) = ss.store_id
  LEFT JOIN (
    SELECT
      CAST(transaction_id AS STRING) AS transaction_id,
      ROW_NUMBER() OVER (PARTITION BY CAST(transaction_id AS STRING) ORDER BY CAST(transaction_time AS TIMESTAMP) DESC) AS rn
    FROM sales_transactions_bronze
  ) dup
    ON CAST(stb.transaction_id AS STRING) = dup.transaction_id
),
product_dq AS (
  SELECT
    CAST(pb.product_id AS STRING) AS record_id,
    CAST('products_bronze' AS STRING) AS data_source,
    CASE
      WHEN pb.product_id IS NULL OR TRIM(CAST(pb.product_id AS STRING)) = '' THEN 'INVALID_MISSING_PRODUCT_ID'
      WHEN COALESCE(LOWER(TRIM(CAST(pb.is_active AS STRING))), '') NOT IN ('true', '1', 'y', 'yes') THEN 'INACTIVE_PRODUCT'
      ELSE 'VALID'
    END AS validation_status,
    CAST(NULL AS DATE) AS validation_date
  FROM products_bronze pb
),
store_dq AS (
  SELECT
    CAST(sb.store_id AS STRING) AS record_id,
    CAST('stores_bronze' AS STRING) AS data_source,
    CASE
      WHEN sb.store_id IS NULL OR TRIM(CAST(sb.store_id AS STRING)) = '' THEN 'INVALID_MISSING_STORE_ID'
      ELSE 'VALID'
    END AS validation_status,
    CAST(NULL AS DATE) AS validation_date
  FROM stores_bronze sb
)
SELECT record_id, data_source, validation_status, validation_date FROM sales_dq
UNION ALL
SELECT record_id, data_source, validation_status, validation_date FROM product_dq
UNION ALL
SELECT record_id, data_source, validation_status, validation_date FROM store_dq
"""
data_quality_silver_df = spark.sql(data_quality_silver_sql)

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(TARGET_PATH + "/data_quality_silver.csv")
)

job.commit()