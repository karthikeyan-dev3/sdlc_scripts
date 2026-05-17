import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: product_master_silver
# ------------------------------------------------------------------------------
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    CAST(pb.product_name AS STRING) AS product_name,
    CAST(pb.category AS STRING) AS category,
    CAST(pb.price AS FLOAT) AS price
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM dedup
WHERE rn = 1
""")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: store_master_silver
# ------------------------------------------------------------------------------
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    CAST(sb.store_name AS STRING) AS store_name,
    CAST(sb.city AS STRING) AS city,
    CAST(sb.state AS STRING) AS state,
    CAST(sb.store_type AS STRING) AS store_type
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  CAST(CONCAT(COALESCE(city, ''), COALESCE(state, '')) AS STRING) AS location,
  store_type
FROM dedup
WHERE rn = 1
""")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: sales_transactions_silver
# ------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS sale_id,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_revenue
  FROM sales_transactions_bronze stb
),
dedup AS (
  SELECT
    sale_id,
    transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY sale_id) AS rn
  FROM base
)
SELECT
  sale_id,
  transaction_date,
  product_id,
  store_id,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: sales_enriched_silver
# ------------------------------------------------------------------------------
sales_enriched_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    sts.sale_id,
    sts.transaction_date,
    sts.product_id,
    sts.store_id,
    CAST(NULL AS STRING) AS customer_id,
    sts.quantity_sold,
    sts.total_revenue
  FROM sales_transactions_silver sts
  INNER JOIN product_master_silver pms
    ON sts.product_id = pms.product_id
  INNER JOIN store_master_silver sms
    ON sts.store_id = sms.store_id
),
dedup AS (
  SELECT
    sale_id,
    transaction_date,
    product_id,
    store_id,
    customer_id,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY sale_id) AS rn
  FROM joined
)
SELECT
  sale_id,
  transaction_date,
  product_id,
  store_id,
  customer_id,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
""")

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: product_performance_silver
# ------------------------------------------------------------------------------
product_performance_silver_df = spark.sql("""
SELECT
  ses.product_id,
  pms.category,
  CAST(SUM(COALESCE(ses.quantity_sold, 0)) AS BIGINT) AS total_quantity_sold,
  CAST(SUM(COALESCE(ses.total_revenue, 0D)) AS DOUBLE) AS total_revenue,
  CAST(
    SUM(COALESCE(ses.total_revenue, 0D)) / NULLIF(SUM(COALESCE(ses.quantity_sold, 0)), 0)
    AS DOUBLE
  ) AS average_price
FROM sales_enriched_silver ses
INNER JOIN product_master_silver pms
  ON ses.product_id = pms.product_id
GROUP BY
  ses.product_id,
  pms.category
""")

(
    product_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_performance_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save: store_performance_silver
# ------------------------------------------------------------------------------
store_performance_silver_df = spark.sql("""
SELECT
  ses.store_id,
  CAST(COUNT(DISTINCT ses.sale_id) AS BIGINT) AS total_transactions,
  CAST(SUM(COALESCE(ses.total_revenue, 0D)) AS DOUBLE) AS total_revenue,
  CAST(SUM(COALESCE(ses.quantity_sold, 0)) AS BIGINT) AS total_quantity_sold
FROM sales_enriched_silver ses
GROUP BY
  ses.store_id
""")

(
    store_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_performance_silver.csv")
)