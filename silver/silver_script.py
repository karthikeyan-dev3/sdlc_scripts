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
# Read source tables from S3 (Bronze)
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
# TARGET: silver.product_silver
# ------------------------------------------------------------
product_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    TRIM(CAST(pb.product_name AS STRING)) AS product_name,
    UPPER(TRIM(CAST(pb.category AS STRING))) AS category,
    UPPER(TRIM(CAST(pb.brand AS STRING))) AS brand,
    CAST(COALESCE(CAST(pb.price AS DOUBLE), 0.0) AS DOUBLE) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM products_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  CASE WHEN price < 0 THEN 0.0 ELSE price END AS price
FROM ranked
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.store_silver
# ------------------------------------------------------------
store_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    TRIM(CAST(sb.store_name AS STRING)) AS store_name,
    TRIM(CAST(sb.state AS STRING)) AS region,
    UPPER(TRIM(CAST(sb.store_type AS STRING))) AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.store_id
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  region,
  store_type
FROM ranked
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.sales_silver
# ------------------------------------------------------------
sales_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(stb.transaction_id AS STRING) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(stb.product_id AS STRING) AS product_id,
    CAST(stb.store_id AS STRING) AS store_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
)
SELECT
  r.transaction_id,
  CAST(r.transaction_time AS DATE) AS sale_date,
  r.product_id,
  r.store_id,
  r.quantity_sold,
  r.total_sales_amount
FROM ranked r
INNER JOIN product_silver ps
  ON r.product_id = ps.product_id
INNER JOIN store_silver ss
  ON r.store_id = ss.store_id
WHERE r.rn = 1
  AND r.quantity_sold > 0
  AND r.total_sales_amount >= 0
"""
sales_silver_df = spark.sql(sales_silver_sql)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.sales_aggregated_silver
# ------------------------------------------------------------
sales_aggregated_silver_sql = """
SELECT
  CAST(sls.store_id AS STRING) AS store_id,
  CAST(sls.product_id AS STRING) AS product_id,
  CAST(sls.sale_date AS DATE) AS sale_date,
  CAST(SUM(CAST(sls.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
  CAST(SUM(CAST(sls.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount
FROM sales_silver sls
GROUP BY
  sls.store_id,
  sls.product_id,
  sls.sale_date
"""
sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.data_quality_silver
# ------------------------------------------------------------
data_quality_silver_sql = """
SELECT
  CAST(sls.transaction_id AS STRING) AS record_id,
  CAST(sls.transaction_id AS DOUBLE) AS data_quality_score,
  CAST(sls.transaction_id AS STRING) AS validation_status,
  CAST(sls.transaction_id AS TIMESTAMP) AS last_update_timestamp
FROM sales_silver sls
"""
data_quality_silver_df = spark.sql(data_quality_silver_sql)
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_silver.csv")
)

job.commit()