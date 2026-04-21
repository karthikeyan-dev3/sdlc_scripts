```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ------------------------------------------------------------------------------------
# AWS Glue boilerplate
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended for CSV ingestion
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT PATH FORMAT)
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
# TARGET TABLE: gold_daily_store_sales
# Mapping: silver.sales_transactions_silver sts INNER JOIN silver.stores_silver ss ON sts.store_id = ss.store_id
# ====================================================================================
gold_daily_store_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                AS sales_date,
        CAST(sts.store_id AS STRING)                                AS store_id,
        CAST(ss.store_name AS STRING)                               AS store_name,
        CAST(ss.city AS STRING)                                     AS city,
        CAST(ss.state AS STRING)                                    AS state,
        CAST(ss.store_type AS STRING)                               AS store_type,
        CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))               AS total_revenue,
        CAST(SUM(sts.quantity) AS INT)                              AS total_quantity_sold,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT)             AS transaction_count
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(ss.store_name AS STRING),
        CAST(ss.city AS STRING),
        CAST(ss.state AS STRING),
        CAST(ss.store_type AS STRING)
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_product_sales
# Mapping: silver.sales_transactions_silver sts INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ====================================================================================
gold_daily_product_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                AS sales_date,
        CAST(sts.product_id AS STRING)                              AS product_id,
        CAST(ps.product_name AS STRING)                             AS product_name,
        CAST(ps.brand AS STRING)                                    AS brand,
        CAST(ps.category AS STRING)                                 AS category,
        CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))               AS total_revenue,
        CAST(SUM(sts.quantity) AS INT)                              AS total_quantity_sold,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT)             AS transaction_count
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(sts.product_id AS STRING),
        CAST(ps.product_name AS STRING),
        CAST(ps.brand AS STRING),
        CAST(ps.category AS STRING)
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_category_sales
# Mapping: silver.sales_transactions_silver sts INNER JOIN silver.products_silver ps ON sts.product_id = ps.product_id
# ====================================================================================
gold_daily_category_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                AS sales_date,
        CAST(ps.category AS STRING)                                 AS category,
        CAST(SUM(sts.sale_amount) AS DECIMAL(38, 10))               AS total_revenue,
        CAST(SUM(sts.quantity) AS INT)                              AS total_quantity_sold,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT)             AS transaction_count
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(ps.category AS STRING)
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_category_sales.csv")
)

job.commit()
```