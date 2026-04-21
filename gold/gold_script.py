```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
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

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    IMPORTANT: path must be .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ===================================================================================
# TARGET TABLE: gold_sales_store_daily
# mapping_details:
#   silver.sales_transactions_silver sts INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ===================================================================================
gold_sales_store_daily_sql = """
SELECT
  DATE(sts.sales_date)                                                AS sales_date,
  CAST(sts.store_id AS STRING)                                        AS store_id,
  CAST(ss.store_name AS STRING)                                       AS store_name,
  CAST(ss.city AS STRING)                                             AS city,
  CAST(ss.state AS STRING)                                            AS state,
  CAST(ss.store_type AS STRING)                                       AS store_type,
  CAST(SUM(CAST(sts.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT)     AS total_transactions,
  CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                         AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  DATE(sts.sales_date),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING),
  CAST(ss.store_type AS STRING)
"""

gold_sales_store_daily_df = spark.sql(gold_sales_store_daily_sql)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_sales_store_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_store_daily.csv")
)

# ===================================================================================
# TARGET TABLE: gold_sales_product_daily
# mapping_details:
#   silver.sales_transactions_silver sts INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ===================================================================================
gold_sales_product_daily_sql = """
SELECT
  DATE(sts.sales_date)                                                AS sales_date,
  CAST(sts.product_id AS STRING)                                      AS product_id,
  CAST(ps.product_name AS STRING)                                     AS product_name,
  CAST(ps.brand AS STRING)                                            AS brand,
  CAST(ps.category AS STRING)                                         AS category,
  CAST(SUM(CAST(sts.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                         AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT)     AS total_transactions
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  DATE(sts.sales_date),
  CAST(sts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.brand AS STRING),
  CAST(ps.category AS STRING)
"""

gold_sales_product_daily_df = spark.sql(gold_sales_product_daily_sql)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_sales_product_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_product_daily.csv")
)

# ===================================================================================
# TARGET TABLE: gold_sales_category_store_daily
# mapping_details:
#   silver.sales_transactions_silver sts
#   INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
#   INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ===================================================================================
gold_sales_category_store_daily_sql = """
SELECT
  DATE(sts.sales_date)                                                AS sales_date,
  CAST(ps.category AS STRING)                                         AS category,
  CAST(sts.store_id AS STRING)                                        AS store_id,
  CAST(ss.store_name AS STRING)                                       AS store_name,
  CAST(ss.city AS STRING)                                             AS city,
  CAST(ss.store_type AS STRING)                                       AS store_type,
  CAST(SUM(CAST(sts.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
  CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                         AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT)     AS total_transactions
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  DATE(sts.sales_date),
  CAST(ps.category AS STRING),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.store_type AS STRING)
"""

gold_sales_category_store_daily_df = spark.sql(gold_sales_category_store_daily_sql)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    gold_sales_category_store_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_category_store_daily.csv")
)

job.commit()
```