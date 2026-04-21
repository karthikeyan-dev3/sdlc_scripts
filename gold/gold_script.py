
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue boilerplate
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    IMPORTANT: Path must be constructed using FILE_FORMAT and end with "/"
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
# mapping_details: silver.sales_transactions_silver sts
#                  LEFT JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ====================================================================================
gold_daily_store_sales_sql = """
WITH base AS (
    SELECT
        DATE(sts.sales_date) AS sales_date,
        CAST(sts.store_id AS STRING) AS store_id,

        -- store attributes
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.state AS STRING) AS state,
        CAST(ss.country AS STRING) AS country,
        CAST(ss.store_type AS STRING) AS store_type,

        -- transaction grain fields
        CAST(sts.sale_amount AS DECIMAL(18,2)) AS sale_amount,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.transaction_id AS STRING) AS transaction_id
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
),
agg AS (
    SELECT
        sales_date,
        store_id,
        store_name,
        city,
        state,
        country,
        store_type,
        SUM(sale_amount) AS total_revenue,
        SUM(quantity) AS total_quantity_sold,
        COUNT(DISTINCT transaction_id) AS total_transactions
    FROM base
    GROUP BY
        sales_date,
        store_id,
        store_name,
        city,
        state,
        country,
        store_type
)
SELECT
    sales_date,
    store_id,
    store_name,
    city,
    state,
    country,
    store_type,
    total_revenue,
    total_quantity_sold,
    CAST(total_transactions AS INT) AS total_transactions
FROM agg
"""

gold_daily_store_sales_df = spark.sql(gold_daily_store_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ====================================================================================
# TARGET TABLE 2: gold_daily_product_sales
# mapping_details: silver.sales_transactions_silver sts
#                  LEFT JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ====================================================================================
gold_daily_product_sales_sql = """
WITH base AS (
    SELECT
        DATE(sts.sales_date) AS sales_date,
        CAST(sts.product_id AS STRING) AS product_id,

        -- product attributes
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.brand AS STRING) AS brand,
        CAST(ps.category AS STRING) AS category,

        -- transaction grain fields
        CAST(sts.sale_amount AS DECIMAL(18,2)) AS sale_amount,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.transaction_id AS STRING) AS transaction_id
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
),
agg AS (
    SELECT
        sales_date,
        product_id,
        product_name,
        brand,
        category,
        SUM(sale_amount) AS total_revenue,
        SUM(quantity) AS total_quantity_sold,
        COUNT(DISTINCT transaction_id) AS total_transactions
    FROM base
    GROUP BY
        sales_date,
        product_id,
        product_name,
        brand,
        category
)
SELECT
    sales_date,
    product_id,
    product_name,
    brand,
    category,
    total_revenue,
    total_quantity_sold,
    CAST(total_transactions AS INT) AS total_transactions
FROM agg
"""

gold_daily_product_sales_df = spark.sql(gold_daily_product_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ====================================================================================
# TARGET TABLE 3: gold_daily_store_category_sales
# mapping_details: silver.sales_transactions_silver sts
#                  LEFT JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
#                  LEFT JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ====================================================================================
gold_daily_store_category_sales_sql = """
WITH base AS (
    SELECT
        DATE(sts.sales_date) AS sales_date,
        CAST(sts.store_id AS STRING) AS store_id,

        -- store attributes
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.store_type AS STRING) AS store_type,

        -- category
        CAST(ps.category AS STRING) AS category,

        -- transaction grain fields
        CAST(sts.sale_amount AS DECIMAL(18,2)) AS sale_amount,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.transaction_id AS STRING) AS transaction_id
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
),
agg AS (
    SELECT
        sales_date,
        store_id,
        store_name,
        city,
        store_type,
        category,
        SUM(sale_amount) AS total_revenue,
        SUM(quantity) AS total_quantity_sold,
        COUNT(DISTINCT transaction_id) AS total_transactions
    FROM base
    GROUP BY
        sales_date,
        store_id,
        store_name,
        city,
        store_type,
        category
)
SELECT
    sales_date,
    store_id,
    store_name,
    city,
    store_type,
    category,
    total_revenue,
    total_quantity_sold,
    CAST(total_transactions AS INT) AS total_transactions
FROM agg
"""

gold_daily_store_category_sales_df = spark.sql(gold_daily_store_category_sales_sql)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    gold_daily_store_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_category_sales.csv")
)

job.commit()
