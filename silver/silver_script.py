
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Read source tables (Bronze)
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# Target: silver.products_silver
# -------------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY
        CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 0 ELSE 1 END
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category
FROM base
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

# -------------------------------------------------------------------
# Target: silver.stores_silver
# -------------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.city) AS city,
    TRIM(sb.state) AS state,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY
        CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 0 ELSE 1 END,
        CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 0 ELSE 1 END
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  city,
  state
FROM base
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

# -------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON stb.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON stb.store_id = ss.store_id
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    quantity,
    sale_amount,
    transaction_time
  FROM joined
  WHERE rn = 1
)
SELECT
  transaction_id,
  product_id,
  store_id,
  CAST(COALESCE(quantity, 0) AS INT) AS quantity,
  CAST(COALESCE(sale_amount, 0.0) AS DOUBLE) AS sale_amount,
  transaction_time
FROM dedup
WHERE COALESCE(quantity, 0) >= 0
  AND COALESCE(sale_amount, 0.0) >= 0
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