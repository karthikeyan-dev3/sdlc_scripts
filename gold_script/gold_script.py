```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
#    (STRICT: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/"))
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ====================================================================================
# TARGET TABLE 1: gold_daily_store_sales
# mapping: silver.sales_transactions_silver stxs LEFT JOIN silver.stores_silver sts
# ====================================================================================
gold_daily_store_sales_sql = """
SELECT
  DATE(stxs.sales_date)                                           AS sales_date,
  CAST(stxs.store_id AS STRING)                                   AS store_id,
  CAST(sts.store_name AS STRING)                                  AS store_name,
  CAST(sts.city AS STRING)                                        AS city,
  CAST(sts.state AS STRING)                                       AS state,
  CAST(sts.country AS STRING)                                     AS country,
  CAST(sts.store_type AS STRING)                                  AS store_type,
  CAST(SUM(CAST(stxs.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
  CAST(SUM(CAST(stxs.quantity AS INT)) AS INT)                    AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(stxs.transaction_id AS STRING)) AS INT) AS transaction_count
FROM sales_transactions_silver stxs
LEFT JOIN stores_silver sts
  ON stxs.store_id = sts.store_id
GROUP BY
  DATE(stxs.sales_date),
  CAST(stxs.store_id AS STRING),
  CAST(sts.store_name AS STRING),
  CAST(sts.city AS STRING),
  CAST(sts.state AS STRING),
  CAST(sts.country AS STRING),
  CAST(sts.store_type AS STRING)
"""

gold_daily_store_sales_df = spark.sql(gold_daily_store_sales_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ====================================================================================
# TARGET TABLE 2: gold_daily_product_sales
# mapping: silver.sales_transactions_silver stxs LEFT JOIN silver.products_silver prs
# ====================================================================================
gold_daily_product_sales_sql = """
SELECT
  DATE(stxs.sales_date)                                           AS sales_date,
  CAST(stxs.product_id AS STRING)                                 AS product_id,
  CAST(prs.product_name AS STRING)                                AS product_name,
  CAST(prs.brand AS STRING)                                       AS brand,
  CAST(prs.category AS STRING)                                    AS category,
  CAST(SUM(CAST(stxs.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
  CAST(SUM(CAST(stxs.quantity AS INT)) AS INT)                    AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(stxs.transaction_id AS STRING)) AS INT) AS transaction_count
FROM sales_transactions_silver stxs
LEFT JOIN products_silver prs
  ON stxs.product_id = prs.product_id
GROUP BY
  DATE(stxs.sales_date),
  CAST(stxs.product_id AS STRING),
  CAST(prs.product_name AS STRING),
  CAST(prs.brand AS STRING),
  CAST(prs.category AS STRING)
"""

gold_daily_product_sales_df = spark.sql(gold_daily_product_sales_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ====================================================================================
# TARGET TABLE 3: gold_daily_store_category_sales
# mapping: stxs LEFT JOIN sts LEFT JOIN prs
# ====================================================================================
gold_daily_store_category_sales_sql = """
SELECT
  DATE(stxs.sales_date)                                           AS sales_date,
  CAST(stxs.store_id AS STRING)                                   AS store_id,
  CAST(sts.store_name AS STRING)                                  AS store_name,
  CAST(sts.city AS STRING)                                        AS city,
  CAST(sts.store_type AS STRING)                                  AS store_type,
  CAST(prs.category AS STRING)                                    AS category,
  CAST(SUM(CAST(stxs.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
  CAST(SUM(CAST(stxs.quantity AS INT)) AS INT)                    AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(stxs.transaction_id AS STRING)) AS INT) AS transaction_count
FROM sales_transactions_silver stxs
LEFT JOIN stores_silver sts
  ON stxs.store_id = sts.store_id
LEFT JOIN products_silver prs
  ON stxs.product_id = prs.product_id
GROUP BY
  DATE(stxs.sales_date),
  CAST(stxs.store_id AS STRING),
  CAST(sts.store_name AS STRING),
  CAST(sts.city AS STRING),
  CAST(sts.store_type AS STRING),
  CAST(prs.category AS STRING)
"""

gold_daily_store_category_sales_df = spark.sql(gold_daily_store_category_sales_sql)

# Write as a SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_store_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_category_sales.csv")
)

job.commit()
```