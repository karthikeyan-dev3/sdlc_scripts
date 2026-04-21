```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    NOTE: Paths MUST follow: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ss_df.createOrReplaceTempView("stores_silver")
ps_df.createOrReplaceTempView("products_silver")

# ===================================================================================
# TARGET TABLE 1: gold.gold_store_daily_sales (gsds)
# ===================================================================================
gsds_sql = """
SELECT
  CAST(sts.sales_date AS DATE)                                         AS sales_date,
  CAST(sts.store_id AS STRING)                                         AS store_id,
  CAST(ss.store_name AS STRING)                                        AS store_name,
  CAST(ss.city AS STRING)                                              AS city,
  CAST(ss.state AS STRING)                                             AS state,
  CAST(ss.country AS STRING)                                           AS country,
  CAST(ss.store_type AS STRING)                                        AS store_type,
  CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))                        AS total_revenue,
  CAST(SUM(sts.quantity) AS INT)                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)                      AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING),
  CAST(ss.country AS STRING),
  CAST(ss.store_type AS STRING)
"""

gsds_df = spark.sql(gsds_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gsds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# ===================================================================================
# TARGET TABLE 2: gold.gold_product_daily_sales (gpds)
# ===================================================================================
gpds_sql = """
SELECT
  CAST(sts.sales_date AS DATE)                                         AS sales_date,
  CAST(sts.product_id AS STRING)                                       AS product_id,
  CAST(ps.product_name AS STRING)                                      AS product_name,
  CAST(ps.brand AS STRING)                                             AS brand,
  CAST(ps.category AS STRING)                                          AS category,
  CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))                        AS total_revenue,
  CAST(SUM(sts.quantity) AS INT)                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)                      AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.brand AS STRING),
  CAST(ps.category AS STRING)
"""

gpds_df = spark.sql(gpds_sql)

(
    gpds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

# ===================================================================================
# TARGET TABLE 3: gold.gold_category_daily_sales (gcds)
# ===================================================================================
gcds_sql = """
SELECT
  CAST(sts.sales_date AS DATE)                                         AS sales_date,
  CAST(ps.category AS STRING)                                          AS category,
  CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))                        AS total_revenue,
  CAST(SUM(sts.quantity) AS INT)                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)                      AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(ps.category AS STRING)
"""

gcds_df = spark.sql(gcds_sql)

(
    gcds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_category_daily_sales.csv")
)

# ===================================================================================
# TARGET TABLE 4: gold.gold_store_category_daily_sales (gscds)
# ===================================================================================
gscds_sql = """
SELECT
  CAST(sts.sales_date AS DATE)                                         AS sales_date,
  CAST(sts.store_id AS STRING)                                         AS store_id,
  CAST(ss.store_name AS STRING)                                        AS store_name,
  CAST(ss.city AS STRING)                                              AS city,
  CAST(ss.store_type AS STRING)                                        AS store_type,
  CAST(ps.category AS STRING)                                          AS category,
  CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))                        AS total_revenue,
  CAST(SUM(sts.quantity) AS INT)                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)                      AS transaction_count
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.sales_date AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.store_type AS STRING),
  CAST(ps.category AS STRING)
"""

gscds_df = spark.sql(gscds_sql)

(
    gscds_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_category_daily_sales.csv")
)

job.commit()
```