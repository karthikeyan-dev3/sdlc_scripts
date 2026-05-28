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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ---------------------------
# 1) Read source tables (S3)
# ---------------------------
product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

# ---------------------------
# 2) Create temp views
# ---------------------------
product_silver_df.createOrReplaceTempView("product_silver")
store_silver_df.createOrReplaceTempView("store_silver")
sales_silver_df.createOrReplaceTempView("sales_silver")

# ============================================================
# Target: gold_product
# Source: silver.product_silver ps
# ============================================================
gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)   AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.price AS DOUBLE)        AS price
FROM product_silver ps
""")

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ============================================================
# Target: gold_store
# Source: silver.store_silver ss
# ============================================================
gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)   AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING)   AS location,
  CAST(ss.store_type AS STRING) AS store_type
FROM store_silver ss
""")

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ============================================================
# Target: gold_sales
# Source: silver.sales_silver sls
# ============================================================
gold_sales_df = spark.sql("""
SELECT
  CAST(sls.sales_id AS STRING)           AS sales_id,
  DATE(CAST(sls.transaction_date AS STRING)) AS transaction_date,
  CAST(sls.store_id AS STRING)           AS store_id,
  CAST(sls.product_id AS STRING)         AS product_id,
  CAST(sls.quantity_sold AS INT)         AS quantity_sold,
  CAST(sls.revenue AS DOUBLE)            AS revenue
FROM sales_silver sls
""")

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target: gold_product_performance
# Source: silver.sales_silver sls INNER JOIN silver.product_silver ps ON sls.product_id = ps.product_id
# ============================================================
gold_product_performance_df = spark.sql("""
WITH agg AS (
  SELECT
    CAST(sls.product_id AS STRING)         AS product_id,
    CAST(ps.product_name AS STRING)        AS product_name,
    CAST(ps.category AS STRING)            AS category,
    CAST(SUM(CAST(sls.revenue AS DOUBLE)) AS DOUBLE)      AS total_revenue,
    CAST(SUM(CAST(sls.quantity_sold AS INT)) AS INT)      AS units_sold
  FROM sales_silver sls
  INNER JOIN product_silver ps
    ON sls.product_id = ps.product_id
  GROUP BY
    sls.product_id,
    ps.product_name,
    ps.category
)
SELECT
  product_id,
  product_name,
  category,
  total_revenue,
  units_sold,
  CASE
    WHEN DENSE_RANK() OVER (ORDER BY total_revenue DESC) <= 10 THEN 'Y'
    ELSE 'N'
  END AS top_selling_flag
FROM agg
""")

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_store_performance
# Source: silver.sales_silver sls INNER JOIN silver.store_silver ss ON sls.store_id = ss.store_id
# ============================================================
gold_store_performance_df = spark.sql("""
SELECT
  CAST(sls.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location,
  CAST(SUM(CAST(sls.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(sls.sales_id AS STRING)) AS INT) AS transaction_count,
  CAST(SUM(CAST(sls.quantity_sold AS INT)) AS INT) AS total_quantity_sold
FROM sales_silver sls
INNER JOIN store_silver ss
  ON sls.store_id = ss.store_id
GROUP BY
  sls.store_id,
  ss.store_name,
  ss.location
""")

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

job.commit()