```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
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
#    IMPORTANT: path must be: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
fact_transaction_line_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/fact_transaction_line_silver.{FILE_FORMAT}/")
)

dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)

dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
fact_transaction_line_silver_df.createOrReplaceTempView("fact_transaction_line_silver")
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")

# ===================================================================================
# TARGET TABLE: gold_daily_store_sales
# Description:
# Aggregate transaction lines to daily store level. Group by ft.sales_date and store
# attributes from ds (store_id, store_name, city, state, store_type). Compute
# total_revenue as sum of ft.sale_amount, total_quantity_sold as sum of ft.quantity,
# and total_transactions as distinct count of ft.transaction_id.
# ===================================================================================
gold_daily_store_sales_sql = """
SELECT
    CAST(ft.sales_date AS DATE)                                   AS sales_date,
    CAST(ds.store_id AS STRING)                                   AS store_id,
    CAST(ds.store_name AS STRING)                                 AS store_name,
    CAST(ds.city AS STRING)                                       AS city,
    CAST(ds.state AS STRING)                                      AS state,
    CAST(ds.store_type AS STRING)                                 AS store_type,
    CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                    AS total_quantity_sold,
    CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT) AS total_transactions
FROM fact_transaction_line_silver ft
INNER JOIN dim_store_silver ds
    ON ft.store_id = ds.store_id
GROUP BY
    CAST(ft.sales_date AS DATE),
    CAST(ds.store_id AS STRING),
    CAST(ds.store_name AS STRING),
    CAST(ds.city AS STRING),
    CAST(ds.state AS STRING),
    CAST(ds.store_type AS STRING)
"""

gold_daily_store_sales_df = spark.sql(gold_daily_store_sales_sql)

(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ===================================================================================
# TARGET TABLE: gold_daily_product_sales
# Description:
# Aggregate transaction lines to daily product level. Group by ft.sales_date and
# product attributes from dp (product_id, product_name, brand, category). Compute
# total_revenue as sum of ft.sale_amount, total_quantity_sold as sum of ft.quantity,
# and total_transactions as distinct count of ft.transaction_id.
# ===================================================================================
gold_daily_product_sales_sql = """
SELECT
    CAST(ft.sales_date AS DATE)                                   AS sales_date,
    CAST(dp.product_id AS STRING)                                 AS product_id,
    CAST(dp.product_name AS STRING)                               AS product_name,
    CAST(dp.brand AS STRING)                                      AS brand,
    CAST(dp.category AS STRING)                                   AS category,
    CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                    AS total_quantity_sold,
    CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT) AS total_transactions
FROM fact_transaction_line_silver ft
INNER JOIN dim_product_silver dp
    ON ft.product_id = dp.product_id
GROUP BY
    CAST(ft.sales_date AS DATE),
    CAST(dp.product_id AS STRING),
    CAST(dp.product_name AS STRING),
    CAST(dp.brand AS STRING),
    CAST(dp.category AS STRING)
"""

gold_daily_product_sales_df = spark.sql(gold_daily_product_sales_sql)

(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ===================================================================================
# TARGET TABLE: gold_daily_store_category_sales
# Description:
# Aggregate transaction lines to daily store-by-category level. Group by ft.sales_date,
# store attributes from ds (store_id, store_name, city, store_type), and dp.category.
# Compute total_revenue as sum of ft.sale_amount, total_quantity_sold as sum of
# ft.quantity, and total_transactions as distinct count of ft.transaction_id.
# ===================================================================================
gold_daily_store_category_sales_sql = """
SELECT
    CAST(ft.sales_date AS DATE)                                   AS sales_date,
    CAST(ds.store_id AS STRING)                                   AS store_id,
    CAST(ds.store_name AS STRING)                                 AS store_name,
    CAST(ds.city AS STRING)                                       AS city,
    CAST(ds.store_type AS STRING)                                 AS store_type,
    CAST(dp.category AS STRING)                                   AS category,
    CAST(SUM(CAST(ft.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ft.quantity AS INT)) AS INT)                    AS total_quantity_sold,
    CAST(COUNT(DISTINCT CAST(ft.transaction_id AS STRING)) AS INT) AS total_transactions
FROM fact_transaction_line_silver ft
INNER JOIN dim_store_silver ds
    ON ft.store_id = ds.store_id
INNER JOIN dim_product_silver dp
    ON ft.product_id = dp.product_id
GROUP BY
    CAST(ft.sales_date AS DATE),
    CAST(ds.store_id AS STRING),
    CAST(ds.store_name AS STRING),
    CAST(ds.city AS STRING),
    CAST(ds.store_type AS STRING),
    CAST(dp.category AS STRING)
"""

gold_daily_store_category_sales_df = spark.sql(gold_daily_store_category_sales_sql)

(
    gold_daily_store_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_daily_store_category_sales.csv")
)

job.commit()
```