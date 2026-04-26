```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Job setup (AWS Glue)
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    SOURCE READING RULE:
#      .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
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

categories_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/categories_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
transactions_silver_df.createOrReplaceTempView("transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
categories_silver_df.createOrReplaceTempView("categories_silver")

# ===================================================================================
# TARGET TABLE 1: gold_store_daily_sales
# Mapping: transactions_silver ts LEFT JOIN stores_silver ss ON ts.store_id = ss.store_id
# Transformations (EXACT from UDT):
#   gsds.sales_date = ts.sales_date
#   gsds.store_id = ts.store_id
#   gsds.store_name = ss.store_name
#   gsds.city = ss.city
#   gsds.state = ss.state
#   gsds.country = ss.country
#   gsds.store_type = ss.store_type
#   gsds.total_revenue = SUM(ts.sale_amount)
#   gsds.total_quantity_sold = SUM(ts.quantity)
#   gsds.transaction_count = COUNT(DISTINCT ts.transaction_id)
# ===================================================================================
gold_store_daily_sales_sql = """
SELECT
  CAST(ts.sales_date AS DATE)                                         AS sales_date,
  CAST(ts.store_id AS STRING)                                         AS store_id,
  CAST(ss.store_name AS STRING)                                       AS store_name,
  CAST(ss.city AS STRING)                                             AS city,
  CAST(ss.state AS STRING)                                            AS state,
  CAST(ss.country AS STRING)                                          AS country,
  CAST(ss.store_type AS STRING)                                       AS store_type,
  SUM(CAST(ts.sale_amount AS DECIMAL(38, 10)))                        AS total_revenue,
  SUM(CAST(ts.quantity AS INT))                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT ts.transaction_id) AS INT)                      AS transaction_count
FROM transactions_silver ts
LEFT JOIN stores_silver ss
  ON ts.store_id = ss.store_id
GROUP BY
  CAST(ts.sales_date AS DATE),
  CAST(ts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING),
  CAST(ss.country AS STRING),
  CAST(ss.store_type AS STRING)
"""

gold_store_daily_sales_df = spark.sql(gold_store_daily_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
gold_store_daily_sales_output_tmp = f"{TARGET_PATH}/gold_store_daily_sales_tmp/"
gold_store_daily_sales_output_final = f"{TARGET_PATH}/gold_store_daily_sales.csv"

(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_store_daily_sales_output_tmp)
)

# Move single part file to required final path (single CSV directly under TARGET_PATH)
_tmp_files_1 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
).listStatus(sc._jvm.org.apache.hadoop.fs.Path(gold_store_daily_sales_output_tmp))
_part_file_1 = None
for f in _tmp_files_1:
    p = f.getPath().getName()
    if p.startswith("part-") and p.endswith(".csv"):
        _part_file_1 = f.getPath().toString()

fs1 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
fs1.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_store_daily_sales_output_final), True)
fs1.rename(
    sc._jvm.org.apache.hadoop.fs.Path(_part_file_1),
    sc._jvm.org.apache.hadoop.fs.Path(gold_store_daily_sales_output_final),
)
fs1.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_store_daily_sales_output_tmp), True)

# ===================================================================================
# TARGET TABLE 2: gold_product_daily_sales
# Mapping: transactions_silver ts LEFT JOIN products_silver ps ON ts.product_id = ps.product_id
# Transformations (EXACT from UDT):
#   gpds.sales_date = ts.sales_date
#   gpds.product_id = ts.product_id
#   gpds.product_name = ps.product_name
#   gpds.brand = ps.brand
#   gpds.category = ps.category
#   gpds.total_revenue = SUM(ts.sale_amount)
#   gpds.total_quantity_sold = SUM(ts.quantity)
#   gpds.transaction_count = COUNT(DISTINCT ts.transaction_id)
# ===================================================================================
gold_product_daily_sales_sql = """
SELECT
  CAST(ts.sales_date AS DATE)                                         AS sales_date,
  CAST(ts.product_id AS STRING)                                       AS product_id,
  CAST(ps.product_name AS STRING)                                     AS product_name,
  CAST(ps.brand AS STRING)                                            AS brand,
  CAST(ps.category AS STRING)                                         AS category,
  SUM(CAST(ts.sale_amount AS DECIMAL(38, 10)))                        AS total_revenue,
  SUM(CAST(ts.quantity AS INT))                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT ts.transaction_id) AS INT)                      AS transaction_count
FROM transactions_silver ts
LEFT JOIN products_silver ps
  ON ts.product_id = ps.product_id
GROUP BY
  CAST(ts.sales_date AS DATE),
  CAST(ts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.brand AS STRING),
  CAST(ps.category AS STRING)
"""

gold_product_daily_sales_df = spark.sql(gold_product_daily_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
gold_product_daily_sales_output_tmp = f"{TARGET_PATH}/gold_product_daily_sales_tmp/"
gold_product_daily_sales_output_final = f"{TARGET_PATH}/gold_product_daily_sales.csv"

(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_product_daily_sales_output_tmp)
)

_tmp_files_2 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
).listStatus(sc._jvm.org.apache.hadoop.fs.Path(gold_product_daily_sales_output_tmp))
_part_file_2 = None
for f in _tmp_files_2:
    p = f.getPath().getName()
    if p.startswith("part-") and p.endswith(".csv"):
        _part_file_2 = f.getPath().toString()

fs2 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
fs2.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_product_daily_sales_output_final), True)
fs2.rename(
    sc._jvm.org.apache.hadoop.fs.Path(_part_file_2),
    sc._jvm.org.apache.hadoop.fs.Path(gold_product_daily_sales_output_final),
)
fs2.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_product_daily_sales_output_tmp), True)

# ===================================================================================
# TARGET TABLE 3: gold_category_daily_sales
# Mapping: transactions_silver ts INNER JOIN categories_silver cs ON ts.category = cs.category
# Transformations (EXACT from UDT):
#   gcds.sales_date = ts.sales_date
#   gcds.category = cs.category
#   gcds.total_revenue = SUM(ts.sale_amount)
#   gcds.total_quantity_sold = SUM(ts.quantity)
#   gcds.transaction_count = COUNT(DISTINCT ts.transaction_id)
# ===================================================================================
gold_category_daily_sales_sql = """
SELECT
  CAST(ts.sales_date AS DATE)                                         AS sales_date,
  CAST(cs.category AS STRING)                                         AS category,
  SUM(CAST(ts.sale_amount AS DECIMAL(38, 10)))                        AS total_revenue,
  SUM(CAST(ts.quantity AS INT))                                       AS total_quantity_sold,
  CAST(COUNT(DISTINCT ts.transaction_id) AS INT)                      AS transaction_count
FROM transactions_silver ts
INNER JOIN categories_silver cs
  ON ts.category = cs.category
GROUP BY
  CAST(ts.sales_date AS DATE),
  CAST(cs.category AS STRING)
"""

gold_category_daily_sales_df = spark.sql(gold_category_daily_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
gold_category_daily_sales_output_tmp = f"{TARGET_PATH}/gold_category_daily_sales_tmp/"
gold_category_daily_sales_output_final = f"{TARGET_PATH}/gold_category_daily_sales.csv"

(
    gold_category_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_category_daily_sales_output_tmp)
)

_tmp_files_3 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
).listStatus(sc._jvm.org.apache.hadoop.fs.Path(gold_category_daily_sales_output_tmp))
_part_file_3 = None
for f in _tmp_files_3:
    p = f.getPath().getName()
    if p.startswith("part-") and p.endswith(".csv"):
        _part_file_3 = f.getPath().toString()

fs3 = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
fs3.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_category_daily_sales_output_final), True)
fs3.rename(
    sc._jvm.org.apache.hadoop.fs.Path(_part_file_3),
    sc._jvm.org.apache.hadoop.fs.Path(gold_category_daily_sales_output_final),
)
fs3.delete(sc._jvm.org.apache.hadoop.fs.Path(gold_category_daily_sales_output_tmp), True)

job.commit()
```