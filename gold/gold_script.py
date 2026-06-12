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

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------

transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
daily_data_quality_runs_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_data_quality_runs_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------

transactions_silver_df.createOrReplaceTempView("transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
daily_data_quality_runs_silver_df.createOrReplaceTempView("daily_data_quality_runs_silver")

# ------------------------------------------------------------------------------
# 3) Transformations (Spark SQL) + 4) Save outputs (single CSV per target)
# ------------------------------------------------------------------------------

# --- gold_store_day_sales ---
gold_store_day_sales_df = spark.sql(
    """
    SELECT
        CAST(ts.sales_date AS DATE) AS sales_date,
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.state AS STRING) AS state,
        CAST(ss.country AS STRING) AS country,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(SUM(CAST(ts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)) AS BIGINT) AS transaction_count,
        CAST(SUM(CAST(ts.quantity AS BIGINT)) AS BIGINT) AS total_qty_sold
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
)

(
    gold_store_day_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_day_sales.csv")
)

# --- gold_product_day_sales ---
gold_product_day_sales_df = spark.sql(
    """
    SELECT
        CAST(ts.sales_date AS DATE) AS sales_date,
        CAST(ts.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category_id AS STRING) AS category_id,
        CAST(ps.category_name AS STRING) AS category_name,
        CAST(ps.brand AS STRING) AS brand,
        CAST(SUM(CAST(ts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(ts.quantity AS BIGINT)) AS BIGINT) AS total_qty_sold,
        CAST(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)) AS BIGINT) AS transaction_count
    FROM transactions_silver ts
    LEFT JOIN products_silver ps
        ON ts.product_id = ps.product_id
    GROUP BY
        CAST(ts.sales_date AS DATE),
        CAST(ts.product_id AS STRING),
        CAST(ps.product_name AS STRING),
        CAST(ps.category_id AS STRING),
        CAST(ps.category_name AS STRING),
        CAST(ps.brand AS STRING)
    """
)

(
    gold_product_day_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_day_sales.csv")
)

# --- gold_store_product_day_sales ---
gold_store_product_day_sales_df = spark.sql(
    """
    SELECT
        CAST(ts.sales_date AS DATE) AS sales_date,
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(ts.product_id AS STRING) AS product_id,
        CAST(ps.category_id AS STRING) AS category_id,
        CAST(SUM(CAST(ts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(ts.quantity AS BIGINT)) AS BIGINT) AS total_qty_sold,
        CAST(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)) AS BIGINT) AS transaction_count
    FROM transactions_silver ts
    LEFT JOIN stores_silver ss
        ON ts.store_id = ss.store_id
    LEFT JOIN products_silver ps
        ON ts.product_id = ps.product_id
    GROUP BY
        CAST(ts.sales_date AS DATE),
        CAST(ts.store_id AS STRING),
        CAST(ss.city AS STRING),
        CAST(ss.store_type AS STRING),
        CAST(ts.product_id AS STRING),
        CAST(ps.category_id AS STRING)
    """
)

(
    gold_store_product_day_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_product_day_sales.csv")
)

# --- gold_data_quality_daily ---
gold_data_quality_daily_df = spark.sql(
    """
    SELECT
        CAST(dq.run_date AS DATE) AS run_date,
        CAST(dq.source_name AS STRING) AS source_name,
        CAST(dq.total_records AS BIGINT) AS total_records,
        CAST(dq.duplicate_records_removed AS BIGINT) AS duplicate_records_removed,
        CAST(dq.invalid_store_id_count AS BIGINT) AS invalid_store_id_count,
        CAST(dq.invalid_product_id_count AS BIGINT) AS invalid_product_id_count
    FROM daily_data_quality_runs_silver dq
    """
)

(
    gold_data_quality_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_daily.csv")
)

job.commit()
