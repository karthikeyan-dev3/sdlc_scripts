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
# Read source tables (Bronze)
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
# Target: silver.products_silver
# ------------------------------------------------------------
products_silver_df = spark.sql("""
WITH base AS (
  SELECT
    UPPER(TRIM(pb.product_id)) AS product_id,
    NULLIF(TRIM(pb.product_name), '') AS product_name,
    NULLIF(UPPER(TRIM(pb.category)), '') AS category,
    CAST(pb.price AS double) AS price
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
ranked AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM ranked
WHERE rn = 1
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.stores_silver
# ------------------------------------------------------------
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    UPPER(TRIM(sb.store_id)) AS store_id,
    NULLIF(TRIM(sb.store_name), '') AS store_name,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE NULL
    END AS region,
    NULLIF(UPPER(TRIM(sb.store_type)), '') AS store_type
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
ranked AS (
  SELECT
    store_id,
    store_name,
    region,
    store_type,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN store_type IS NOT NULL THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region,
  store_type
FROM ranked
WHERE rn = 1
""")
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    UPPER(TRIM(stb.transaction_id)) AS transaction_id,
    CAST(stb.transaction_time AS date) AS date,
    UPPER(TRIM(stb.store_id)) AS store_id,
    UPPER(TRIM(stb.product_id)) AS product_id,
    CAST(stb.quantity AS int) AS quantity,
    COALESCE(
      CAST(stb.sale_amount AS double),
      CAST(stb.quantity AS double) * CAST(ps.price AS double)
    ) AS revenue,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  LEFT JOIN products_silver ps
    ON UPPER(TRIM(stb.product_id)) = ps.product_id
  LEFT JOIN stores_silver ss
    ON UPPER(TRIM(stb.store_id)) = ss.store_id
  WHERE stb.transaction_id IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
),
filtered AS (
  SELECT
    transaction_id,
    date,
    store_id,
    product_id,
    quantity,
    revenue,
    transaction_time
  FROM base
  WHERE quantity > 0
    AND revenue >= 0
),
ranked AS (
  SELECT
    transaction_id,
    date,
    store_id,
    product_id,
    quantity,
    revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM filtered
)
SELECT
  transaction_id,
  date,
  store_id,
  product_id,
  quantity,
  revenue
FROM ranked
WHERE rn = 1
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()