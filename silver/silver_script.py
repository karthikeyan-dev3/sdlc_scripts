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
# Read Source Tables (Bronze)
# ----------------------------
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

# ============================================================
# TARGET TABLE: silver.product_master_silver
# - De-duplicate by product_id using ROW_NUMBER
# - Standardize text fields (TRIM/UPPER)
# - Enforce valid price (>=0)
# - Filter active products (is_active = true)
# ============================================================
product_master_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)                          AS product_id,
    CAST(TRIM(pb.product_name) AS STRING)                        AS product_name,
    CAST(UPPER(TRIM(pb.category)) AS STRING)                     AS category,
    CAST(UPPER(TRIM(pb.brand)) AS STRING)                        AS brand,
    CAST(pb.price AS DOUBLE)                                     AS price,
    pb.is_active                                                 AS is_active
  FROM products_bronze pb
),
ranked AS (
  SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  CAST(price AS DOUBLE) AS price
FROM ranked
WHERE rn = 1
  AND CAST(is_active AS BOOLEAN) = TRUE
  AND (price IS NULL OR price >= 0)
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

# ============================================================
# TARGET TABLE: silver.store_master_silver
# - De-duplicate by store_id using ROW_NUMBER
# - Standardize store_name/city/state formatting
# - Derive location = concat(city, ', ', state)
# - Derive region from state via mapping
# ============================================================
store_master_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING)                             AS store_id,
    CAST(TRIM(sb.store_name) AS STRING)                           AS store_name,
    CAST(UPPER(TRIM(sb.city)) AS STRING)                          AS city,
    CAST(UPPER(TRIM(sb.state)) AS STRING)                         AS state,
    CAST(TRIM(sb.store_type) AS STRING)                           AS store_type
  FROM stores_bronze sb
),
ranked AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN city IS NOT NULL AND TRIM(city) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN state IS NOT NULL AND TRIM(state) <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN store_type IS NOT NULL AND TRIM(store_type) <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
),
final AS (
  SELECT
    store_id,
    store_name,
    CONCAT(city, ', ', state) AS location,
    CASE
      WHEN state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE NULL
    END AS region,
    store_type
  FROM ranked
  WHERE rn = 1
)
SELECT
  store_id,
  store_name,
  location,
  region,
  store_type
FROM final
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

# ============================================================
# TARGET TABLE: silver.sales_transactions_silver
# - De-duplicate by transaction_id using ROW_NUMBER
# - Enforce valid quantity (>0) and sale_amount (>=0)
# - Derive transaction_date = CAST(transaction_time AS DATE)
# - Keep only valid product_id/store_id via joins to conformed masters
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING)                      AS transaction_id,
    CAST(TRIM(stb.product_id) AS STRING)                          AS product_id,
    CAST(TRIM(stb.store_id) AS STRING)                            AS store_id,
    CAST(stb.transaction_time AS TIMESTAMP)                       AS transaction_time,
    CAST(stb.quantity AS INT)                                     AS quantity,
    CAST(stb.sale_amount AS DOUBLE)                               AS sale_amount
  FROM sales_transactions_bronze stb
),
joined AS (
  SELECT
    b.transaction_id,
    pms.product_id                                                AS product_id,
    sms.store_id                                                  AS store_id,
    CAST(b.transaction_time AS DATE)                              AS transaction_date,
    b.quantity                                                    AS quantity_sold,
    b.sale_amount                                                 AS total_sales_amount
  FROM base b
  INNER JOIN product_master_silver pms
    ON b.product_id = pms.product_id
  INNER JOIN store_master_silver sms
    ON b.store_id = sms.store_id
),
ranked AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY
        CASE WHEN transaction_date IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN quantity_sold IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN total_sales_amount IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM joined
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_date,
  quantity_sold,
  total_sales_amount
FROM ranked
WHERE rn = 1
  AND quantity_sold > 0
  AND total_sales_amount >= 0
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
