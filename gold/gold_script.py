
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Job Setup
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
# 1) Read Source Tables from S3
# -----------------------------------------------------------------------------------
# Source: silver.sales_enriched_silver
sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.csv/")
)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

# -----------------------------------------------------------------------------------
# 2) Transform & Write: gold.gold_sales_store_daily
#    GROUP BY sales_date, store_id, store_name, city, state, country, store_type
# -----------------------------------------------------------------------------------
gold_sales_store_daily_df = spark.sql("""
SELECT
    DATE(ses.sales_date)                                  AS sales_date,
    CAST(ses.store_id AS STRING)                          AS store_id,
    CAST(ses.store_name AS STRING)                        AS store_name,
    CAST(ses.city AS STRING)                              AS city,
    CAST(ses.state AS STRING)                             AS state,
    CAST(ses.country AS STRING)                           AS country,
    CAST(ses.store_type AS STRING)                        AS store_type,
    CAST(SUM(CAST(ses.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ses.quantity AS INT)) AS INT)           AS total_quantity_sold,
    CAST(COUNT(DISTINCT ses.transaction_id) AS INT)       AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    DATE(ses.sales_date),
    CAST(ses.store_id AS STRING),
    CAST(ses.store_name AS STRING),
    CAST(ses.city AS STRING),
    CAST(ses.state AS STRING),
    CAST(ses.country AS STRING),
    CAST(ses.store_type AS STRING)
""")

(
    gold_sales_store_daily_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_store_daily.csv")
)

# -----------------------------------------------------------------------------------
# 3) Transform & Write: gold.gold_sales_product_daily
#    GROUP BY sales_date, product_id, product_name, brand, category_std
# -----------------------------------------------------------------------------------
gold_sales_product_daily_df = spark.sql("""
SELECT
    DATE(ses.sales_date)                                  AS sales_date,
    CAST(ses.product_id AS STRING)                        AS product_id,
    CAST(ses.product_name AS STRING)                      AS product_name,
    CAST(ses.brand AS STRING)                             AS brand,
    CAST(ses.category_std AS STRING)                      AS category_std,
    CAST(SUM(CAST(ses.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ses.quantity AS INT)) AS INT)           AS total_quantity_sold,
    CAST(COUNT(DISTINCT ses.transaction_id) AS INT)       AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    DATE(ses.sales_date),
    CAST(ses.product_id AS STRING),
    CAST(ses.product_name AS STRING),
    CAST(ses.brand AS STRING),
    CAST(ses.category_std AS STRING)
""")

(
    gold_sales_product_daily_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_product_daily.csv")
)

# -----------------------------------------------------------------------------------
# 4) Transform & Write: gold.gold_sales_category_store_daily
#    GROUP BY sales_date, category_std, store_id, store_name, city, store_type
# -----------------------------------------------------------------------------------
gold_sales_category_store_daily_df = spark.sql("""
SELECT
    DATE(ses.sales_date)                                  AS sales_date,
    CAST(ses.category_std AS STRING)                      AS category_std,
    CAST(ses.store_id AS STRING)                          AS store_id,
    CAST(ses.store_name AS STRING)                        AS store_name,
    CAST(ses.city AS STRING)                              AS city,
    CAST(ses.store_type AS STRING)                        AS store_type,
    CAST(SUM(CAST(ses.sale_amount AS DECIMAL(38, 10))) AS DECIMAL(38, 10)) AS total_revenue,
    CAST(SUM(CAST(ses.quantity AS INT)) AS INT)           AS total_quantity_sold,
    CAST(COUNT(DISTINCT ses.transaction_id) AS INT)       AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    DATE(ses.sales_date),
    CAST(ses.category_std AS STRING),
    CAST(ses.store_id AS STRING),
    CAST(ses.store_name AS STRING),
    CAST(ses.city AS STRING),
    CAST(ses.store_type AS STRING)
""")

(
    gold_sales_category_store_daily_df
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_category_store_daily.csv")
)

job.commit()
