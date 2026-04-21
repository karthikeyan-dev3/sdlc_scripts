```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ======================================================================================
# AWS Glue / Spark Session
# ======================================================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
spark.sparkContext.setLogLevel("INFO")

# ======================================================================================
# Config
# ======================================================================================
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ======================================================================================
# 1) Read Source Tables from S3
#    SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ======================================================================================
fact_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/fact_transactions_silver.{FILE_FORMAT}/")
)

dim_stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_stores_silver.{FILE_FORMAT}/")
)

dim_products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_products_silver.{FILE_FORMAT}/")
)

# ======================================================================================
# 2) Create Temp Views
# ======================================================================================
fact_transactions_silver_df.createOrReplaceTempView("fact_transactions_silver")
dim_stores_silver_df.createOrReplaceTempView("dim_stores_silver")
dim_products_silver_df.createOrReplaceTempView("dim_products_silver")

# ======================================================================================
# TARGET TABLE: gold_daily_store_sales
# Mapping: silver.fact_transactions_silver ft INNER JOIN silver.dim_stores_silver ds ON ft.store_id = ds.store_id
# ======================================================================================
gold_daily_store_sales_sql = """
SELECT
  CAST(ft.sales_date AS DATE)                                             AS sales_date,
  CAST(ft.store_id AS STRING)                                             AS store_id,
  CAST(ds.store_name AS STRING)                                           AS store_name,
  CAST(ds.city AS STRING)                                                 AS city,
  CAST(ds.store_type AS STRING)                                           AS store_type,
  CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10))   AS total_revenue,
  CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                              AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT)          AS transaction_count
FROM fact_transactions_silver ft
INNER JOIN dim_stores_silver ds
  ON CAST(ft.store_id AS STRING) = CAST(ds.store_id AS STRING)
GROUP BY
  CAST(ft.sales_date AS DATE),
  CAST(ft.store_id AS STRING),
  CAST(ds.store_name AS STRING),
  CAST(ds.city AS STRING),
  CAST(ds.store_type AS STRING)
"""

gold_daily_store_sales_df = spark.sql(gold_daily_store_sales_sql)

# Write SINGLE CSV file directly under TARGET_PATH as TARGET_PATH + "/" + target_table.csv
(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ======================================================================================
# TARGET TABLE: gold_daily_product_sales
# Mapping: silver.fact_transactions_silver ft INNER JOIN silver.dim_products_silver dp ON ft.product_id = dp.product_id
# ======================================================================================
gold_daily_product_sales_sql = """
SELECT
  CAST(ft.sales_date AS DATE)                                             AS sales_date,
  CAST(ft.product_id AS STRING)                                           AS product_id,
  CAST(dp.product_name AS STRING)                                         AS product_name,
  CAST(dp.category AS STRING)                                             AS category,
  CAST(dp.brand AS STRING)                                                AS brand,
  CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10))   AS total_revenue,
  CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                              AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT)          AS transaction_count
FROM fact_transactions_silver ft
INNER JOIN dim_products_silver dp
  ON CAST(ft.product_id AS STRING) = CAST(dp.product_id AS STRING)
GROUP BY
  CAST(ft.sales_date AS DATE),
  CAST(ft.product_id AS STRING),
  CAST(dp.product_name AS STRING),
  CAST(dp.category AS STRING),
  CAST(dp.brand AS STRING)
"""

gold_daily_product_sales_df = spark.sql(gold_daily_product_sales_sql)

(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ======================================================================================
# TARGET TABLE: gold_daily_category_sales
# Mapping: silver.fact_transactions_silver ft INNER JOIN silver.dim_products_silver dp ON ft.product_id = dp.product_id
# ======================================================================================
gold_daily_category_sales_sql = """
SELECT
  CAST(ft.sales_date AS DATE)                                             AS sales_date,
  CAST(dp.category AS STRING)                                             AS category,
  CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10))   AS total_revenue,
  CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                              AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT)          AS transaction_count
FROM fact_transactions_silver ft
INNER JOIN dim_products_silver dp
  ON CAST(ft.product_id AS STRING) = CAST(dp.product_id AS STRING)
GROUP BY
  CAST(ft.sales_date AS DATE),
  CAST(dp.category AS STRING)
"""

gold_daily_category_sales_df = spark.sql(gold_daily_category_sales_sql)

(
    gold_daily_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_category_sales.csv")
)

# ======================================================================================
# TARGET TABLE: gold_daily_store_category_sales
# Mapping:
#   silver.fact_transactions_silver ft
#   INNER JOIN silver.dim_stores_silver ds ON ft.store_id = ds.store_id
#   INNER JOIN silver.dim_products_silver dp ON ft.product_id = dp.product_id
# ======================================================================================
gold_daily_store_category_sales_sql = """
SELECT
  CAST(ft.sales_date AS DATE)                                             AS sales_date,
  CAST(ft.store_id AS STRING)                                             AS store_id,
  CAST(ds.store_name AS STRING)                                           AS store_name,
  CAST(ds.city AS STRING)                                                 AS city,
  CAST(ds.store_type AS STRING)                                           AS store_type,
  CAST(dp.category AS STRING)                                             AS category,
  CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10))   AS total_revenue,
  CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                              AS total_quantity_sold,
  CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT)          AS transaction_count
FROM fact_transactions_silver ft
INNER JOIN dim_stores_silver ds
  ON CAST(ft.store_id AS STRING) = CAST(ds.store_id AS STRING)
INNER JOIN dim_products_silver dp
  ON CAST(ft.product_id AS STRING) = CAST(dp.product_id AS STRING)
GROUP BY
  CAST(ft.sales_date AS DATE),
  CAST(ft.store_id AS STRING),
  CAST(ds.store_name AS STRING),
  CAST(ds.city AS STRING),
  CAST(ds.store_type AS STRING),
  CAST(dp.category AS STRING)
"""

gold_daily_store_category_sales_df = spark.sql(gold_daily_store_category_sales_sql)

(
    gold_daily_store_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_category_sales.csv")
)
```