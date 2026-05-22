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
# 1) Read source tables from S3 (Bronze)
# =============================================================================
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

# =============================================================================
# 2) Create temp views
# =============================================================================
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# =============================================================================
# 3) Transform & Write: silver.sales_transactions_silver
# =============================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING)            AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP)            AS transaction_timestamp,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS transaction_date,
    CAST(TRIM(stb.store_id) AS STRING)                 AS store_id,
    CAST(TRIM(stb.product_id) AS STRING)               AS product_id,
    CAST(COALESCE(CAST(stb.quantity AS INT), 0) AS INT) AS quantity_sold,
    CAST(COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) AS DOUBLE) AS sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(stb.transaction_id) AS STRING)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
),
filtered AS (
  SELECT
    transaction_id,
    transaction_timestamp,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    sales_amount
  FROM base
  WHERE rn = 1
    AND quantity_sold >= 0
    AND sales_amount >= 0
)
SELECT
  transaction_id,
  transaction_timestamp,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  sales_amount
FROM filtered
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

# =============================================================================
# 4) Transform & Write: silver.products_silver
# =============================================================================
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(
      COALESCE(NULLIF(TRIM(pb.product_name), ''), 'UNKNOWN')
      AS STRING
    ) AS product_name,
    CAST(TRIM(pb.category) AS STRING) AS category,
    CAST(TRIM(pb.brand) AS STRING) AS brand,
    CAST(pb.price AS FLOAT) AS price,
    CAST(pb.is_active AS BOOLEAN) AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
      ORDER BY CAST(pb.is_active AS BOOLEAN) DESC
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM base
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# =============================================================================
# 5) Transform & Write: silver.stores_silver
# =============================================================================
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name,
    CAST(TRIM(sb.city) AS STRING) AS city,
    CAST(TRIM(sb.state) AS STRING) AS state,
    CAST(TRIM(sb.store_type) AS STRING) AS store_type,
    CAST(sb.open_date AS DATE) AS open_date,
    CAST(
      COALESCE(
        CASE UPPER(TRIM(sb.state))
          WHEN 'CT' THEN 'NORTHEAST' WHEN 'ME' THEN 'NORTHEAST' WHEN 'MA' THEN 'NORTHEAST'
          WHEN 'NH' THEN 'NORTHEAST' WHEN 'RI' THEN 'NORTHEAST' WHEN 'VT' THEN 'NORTHEAST'
          WHEN 'NJ' THEN 'NORTHEAST' WHEN 'NY' THEN 'NORTHEAST' WHEN 'PA' THEN 'NORTHEAST'
          WHEN 'IL' THEN 'MIDWEST' WHEN 'IN' THEN 'MIDWEST' WHEN 'MI' THEN 'MIDWEST'
          WHEN 'OH' THEN 'MIDWEST' WHEN 'WI' THEN 'MIDWEST' WHEN 'IA' THEN 'MIDWEST'
          WHEN 'KS' THEN 'MIDWEST' WHEN 'MN' THEN 'MIDWEST' WHEN 'MO' THEN 'MIDWEST'
          WHEN 'NE' THEN 'MIDWEST' WHEN 'ND' THEN 'MIDWEST' WHEN 'SD' THEN 'MIDWEST'
          WHEN 'DE' THEN 'SOUTH' WHEN 'FL' THEN 'SOUTH' WHEN 'GA' THEN 'SOUTH'
          WHEN 'MD' THEN 'SOUTH' WHEN 'NC' THEN 'SOUTH' WHEN 'SC' THEN 'SOUTH'
          WHEN 'VA' THEN 'SOUTH' WHEN 'DC' THEN 'SOUTH' WHEN 'WV' THEN 'SOUTH'
          WHEN 'AL' THEN 'SOUTH' WHEN 'KY' THEN 'SOUTH' WHEN 'MS' THEN 'SOUTH'
          WHEN 'TN' THEN 'SOUTH' WHEN 'AR' THEN 'SOUTH' WHEN 'LA' THEN 'SOUTH'
          WHEN 'OK' THEN 'SOUTH' WHEN 'TX' THEN 'SOUTH'
          WHEN 'AZ' THEN 'WEST' WHEN 'CO' THEN 'WEST' WHEN 'ID' THEN 'WEST'
          WHEN 'MT' THEN 'WEST' WHEN 'NV' THEN 'WEST' WHEN 'NM' THEN 'WEST'
          WHEN 'UT' THEN 'WEST' WHEN 'WY' THEN 'WEST'
          WHEN 'AK' THEN 'WEST' WHEN 'CA' THEN 'WEST' WHEN 'HI' THEN 'WEST'
          WHEN 'OR' THEN 'WEST' WHEN 'WA' THEN 'WEST'
          ELSE NULL
        END,
        'UNKNOWN'
      ) AS STRING
    ) AS region,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(sb.store_id) AS STRING)
      ORDER BY CAST(sb.open_date AS DATE) DESC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  city,
  state,
  region,
  store_type,
  open_date
FROM base
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# =============================================================================
# 6) Transform & Write: silver.aggregated_sales_silver
# =============================================================================
aggregated_sales_silver_sql = """
WITH agg AS (
  SELECT
    CAST(sts.transaction_date AS DATE) AS report_date,
    CAST(sts.store_id AS STRING) AS store_id,
    CAST(sts.product_id AS STRING) AS product_id,
    CAST(SUM(CAST(sts.sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
    CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_units_sold,
    CAST(COUNT(DISTINCT sts.transaction_id) AS BIGINT) AS transaction_count
  FROM sales_transactions_silver sts
  GROUP BY
    CAST(sts.transaction_date AS DATE),
    CAST(sts.store_id AS STRING),
    CAST(sts.product_id AS STRING)
)
SELECT
  report_date,
  store_id,
  product_id,
  total_sales,
  total_units_sold,
  transaction_count,
  CAST(
    CASE
      WHEN transaction_count > 0 THEN total_sales / transaction_count
      ELSE NULL
    END AS DOUBLE
  ) AS average_transaction_value,
  CAST(
    CASE
      WHEN total_units_sold > 0 THEN total_sales / total_units_sold
      ELSE NULL
    END AS DOUBLE
  ) AS average_price
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

aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# =============================================================================
# 7) Transform & Write: silver.sales_enriched_silver
# =============================================================================
sales_enriched_silver_sql = """
SELECT
  sts.transaction_id AS transaction_id,
  sts.transaction_date AS transaction_date,
  sts.product_id AS product_id,
  ps.product_name AS product_name,
  sts.store_id AS store_id,
  ss.store_name AS store_name,
  sts.quantity_sold AS quantity_sold,
  sts.sales_amount AS sales_amount,
  ps.category AS category,
  ss.region AS region
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""

sales_enriched_silver_df = spark.sql(sales_enriched_silver_sql)

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

job.commit()