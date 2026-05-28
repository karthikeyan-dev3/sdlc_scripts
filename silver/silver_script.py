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

# ------------------------------------------------------------
# Read Source Tables (Bronze)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Target: products_silver
# ------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(pb.product_id)) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    TRIM(pb.brand) AS brand,
    CAST(pb.price AS double) AS base_price,
    COALESCE(CAST(pb.is_active AS boolean), true) AS is_active
  FROM products_bronze pb
  WHERE TRIM(UPPER(pb.product_id)) IS NOT NULL
    AND TRIM(UPPER(pb.product_id)) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    product_category,
    brand,
    base_price,
    is_active,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN is_active = true THEN 1 ELSE 0 END DESC,
        base_price DESC,
        product_name ASC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  product_category,
  brand,
  base_price,
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

# ------------------------------------------------------------
# Target: stores_silver
# ------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(sb.store_id)) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT_WS(', ', TRIM(sb.city), TRIM(sb.state)) AS store_location,
    TRIM(sb.city) AS city,
    TRIM(sb.state) AS state,
    TRIM(sb.store_type) AS store_type,
    sb.open_date AS open_date
  FROM stores_bronze sb
  WHERE TRIM(UPPER(sb.store_id)) IS NOT NULL
    AND TRIM(UPPER(sb.store_id)) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_location,
    city,
    state,
    store_type,
    open_date,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
        open_date ASC,
        store_name ASC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  store_location,
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

# ------------------------------------------------------------
# Target: sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(stb.transaction_id)) AS transaction_id,
    TRIM(UPPER(stb.product_id)) AS product_id,
    TRIM(UPPER(stb.store_id)) AS store_id,
    stb.transaction_time AS transaction_ts,
    CAST(stb.transaction_time AS date) AS transaction_date,
    CASE
      WHEN stb.quantity IS NULL OR CAST(stb.quantity AS int) < 0 THEN 0
      ELSE CAST(stb.quantity AS int)
    END AS quantity_sold,
    CASE
      WHEN stb.sale_amount IS NULL OR CAST(stb.sale_amount AS double) < 0 THEN 0
      ELSE CAST(stb.sale_amount AS double)
    END AS total_revenue
  FROM sales_transactions_bronze stb
  WHERE TRIM(UPPER(stb.transaction_id)) IS NOT NULL
    AND TRIM(UPPER(stb.transaction_id)) <> ''
),
enriched AS (
  SELECT
    b.transaction_id,
    b.product_id,
    b.store_id,
    b.transaction_ts,
    b.transaction_date,
    b.quantity_sold,
    b.total_revenue,
    CASE
      WHEN b.quantity_sold > 0 THEN b.total_revenue / b.quantity_sold
      ELSE NULL
    END AS unit_revenue,
    CASE WHEN ps.product_id IS NULL THEN true ELSE false END AS invalid_product,
    CASE WHEN ss.store_id IS NULL THEN true ELSE false END AS invalid_store
  FROM base b
  LEFT JOIN products_silver ps
    ON b.product_id = ps.product_id
  LEFT JOIN stores_silver ss
    ON b.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_ts,
    transaction_date,
    quantity_sold,
    total_revenue,
    unit_revenue,
    invalid_product,
    invalid_store,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_ts DESC, total_revenue DESC
    ) AS rn
  FROM enriched
)
SELECT
  transaction_id,
  product_id,
  store_id,
  transaction_ts,
  transaction_date,
  quantity_sold,
  total_revenue,
  unit_revenue,
  invalid_product,
  invalid_store
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
# Target: aggregated_sales_silver
# ------------------------------------------------------------
aggregated_sales_silver_sql = """
WITH agg AS (
  SELECT
    sts.transaction_date AS date,
    sts.store_id AS store_id,
    sts.product_id AS product_id,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    SUM(sts.total_revenue) AS total_revenue,
    COUNT(DISTINCT sts.transaction_id) AS number_of_transactions
  FROM sales_transactions_silver sts
  GROUP BY
    sts.transaction_date,
    sts.store_id,
    sts.product_id
)
SELECT
  date,
  store_id,
  product_id,
  total_quantity_sold,
  total_revenue,
  number_of_transactions,
  CASE
    WHEN number_of_transactions > 0 THEN total_revenue / number_of_transactions
    ELSE 0
  END AS avg_revenue_per_transaction
FROM agg
"""
aggregated_sales_silver_df = spark.sql(aggregated_sales_silver_sql)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
