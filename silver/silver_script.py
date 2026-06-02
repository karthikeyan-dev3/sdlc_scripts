import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read Source Tables (Bronze)
# ------------------------------------------------------------
product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")

store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# Target: silver.product_master_silver
# ------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
  SELECT
    pmb.product_id AS product_id,
    pmb.product_name AS product_name,
    pmb.category AS category,
    CAST(pmb.price AS FLOAT) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pmb.product_id
      ORDER BY
        CASE WHEN pmb.product_name IS NOT NULL AND TRIM(pmb.product_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN pmb.category IS NOT NULL AND TRIM(pmb.category) <> '' THEN 0 ELSE 1 END,
        CASE WHEN pmb.price IS NOT NULL THEN 0 ELSE 1 END
    ) AS rn
  FROM product_master_bronze pmb
)
SELECT
  product_id,
  NULLIF(TRIM(product_name), '') AS product_name,
  NULLIF(TRIM(category), '') AS category,
  CASE
    WHEN price < 0 THEN CAST(0 AS FLOAT)
    ELSE price
  END AS price
FROM base
WHERE rn = 1
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

# ------------------------------------------------------------
# Target: silver.store_master_silver
# ------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
  SELECT
    smb.store_id AS store_id,
    smb.store_name AS store_name,
    smb.city AS city,
    smb.state AS state,
    ROW_NUMBER() OVER (
      PARTITION BY smb.store_id
      ORDER BY
        CASE WHEN smb.store_name IS NOT NULL AND TRIM(smb.store_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN smb.city IS NOT NULL AND TRIM(smb.city) <> '' THEN 0 ELSE 1 END,
        CASE WHEN smb.state IS NOT NULL AND TRIM(smb.state) <> '' THEN 0 ELSE 1 END
    ) AS rn
  FROM store_master_bronze smb
)
SELECT
  store_id,
  NULLIF(TRIM(store_name), '') AS store_name,
  CONCAT(NULLIF(TRIM(city), ''), ', ', NULLIF(TRIM(state), '')) AS location,
  CASE
    WHEN UPPER(TRIM(state)) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
    WHEN UPPER(TRIM(state)) IN ('OH','MI','IN','IL','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
    WHEN UPPER(TRIM(state)) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'SOUTH'
    WHEN UPPER(TRIM(state)) IN ('ID','MT','WY','NV','UT','CO','AZ','NM','AK','WA','OR','CA','HI') THEN 'WEST'
    ELSE NULL
  END AS region
FROM base
WHERE rn = 1
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

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH dedup AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    stb.store_id AS store_id,
    stb.product_id AS product_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sale_amount,
    stb.transaction_id AS customer_id,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  LEFT JOIN store_master_silver sms
    ON stb.store_id = sms.store_id
  LEFT JOIN product_master_silver pms
    ON stb.product_id = pms.product_id
  WHERE stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  CASE
    WHEN quantity_sold < 0 THEN CAST(0 AS INT)
    ELSE quantity_sold
  END AS quantity_sold,
  CASE
    WHEN total_sale_amount < 0 THEN CAST(0 AS DOUBLE)
    ELSE total_sale_amount
  END AS total_sale_amount,
  customer_id
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)
