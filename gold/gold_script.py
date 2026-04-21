```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------------------------------------------------------------
# AWS Glue bootstrap
# ----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------------------------------------------------------------
# Params (as provided)
# ----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------------------------------------------------------------
# 1) Read source tables from S3
#    SOURCE READING RULE:
#      .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ----------------------------------------------------------------------------------
sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)

# ----------------------------------------------------------------------------------
# 2) Create temp views
# ----------------------------------------------------------------------------------
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

# ==================================================================================
# TARGET TABLE: gold_daily_store_sales
# ==================================================================================
gold_daily_store_sales_df = spark.sql("""
SELECT
    CAST(ses.sales_date AS DATE)                                      AS sales_date,
    CAST(ses.store_id AS STRING)                                      AS store_id,
    CAST(ses.store_name AS STRING)                                    AS store_name,
    CAST(ses.city AS STRING)                                          AS city,
    CAST(ses.store_type AS STRING)                                    AS store_type,
    SUM(CAST(ses.sale_amount AS DECIMAL(38, 10)))                     AS total_revenue,
    COUNT(DISTINCT CAST(ses.transaction_id AS STRING))                AS transaction_count,
    SUM(CAST(ses.quantity AS INT))                                    AS total_quantity
FROM sales_enriched_silver ses
GROUP BY
    CAST(ses.sales_date AS DATE),
    CAST(ses.store_id AS STRING),
    CAST(ses.store_name AS STRING),
    CAST(ses.city AS STRING),
    CAST(ses.store_type AS STRING)
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_sales.csv")
)

# ==================================================================================
# TARGET TABLE: gold_daily_product_sales
# ==================================================================================
gold_daily_product_sales_df = spark.sql("""
SELECT
    CAST(ses.sales_date AS DATE)                                      AS sales_date,
    CAST(ses.product_id AS STRING)                                    AS product_id,
    CAST(ses.product_name AS STRING)                                  AS product_name,
    CAST(ses.category AS STRING)                                      AS category,
    CAST(ses.brand AS STRING)                                         AS brand,
    SUM(CAST(ses.sale_amount AS DECIMAL(38, 10)))                     AS total_revenue,
    SUM(CAST(ses.quantity AS INT))                                    AS total_quantity,
    COUNT(DISTINCT CAST(ses.transaction_id AS STRING))                AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    CAST(ses.sales_date AS DATE),
    CAST(ses.product_id AS STRING),
    CAST(ses.product_name AS STRING),
    CAST(ses.category AS STRING),
    CAST(ses.brand AS STRING)
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_product_sales.csv")
)

# ==================================================================================
# TARGET TABLE: gold_daily_category_sales
# ==================================================================================
gold_daily_category_sales_df = spark.sql("""
SELECT
    CAST(ses.sales_date AS DATE)                                      AS sales_date,
    CAST(ses.category AS STRING)                                      AS category,
    SUM(CAST(ses.sale_amount AS DECIMAL(38, 10)))                     AS total_revenue,
    SUM(CAST(ses.quantity AS INT))                                    AS total_quantity,
    COUNT(DISTINCT CAST(ses.transaction_id AS STRING))                AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    CAST(ses.sales_date AS DATE),
    CAST(ses.category AS STRING)
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_category_sales.csv")
)

# ==================================================================================
# TARGET TABLE: gold_daily_store_category_sales
# ==================================================================================
gold_daily_store_category_sales_df = spark.sql("""
SELECT
    CAST(ses.sales_date AS DATE)                                      AS sales_date,
    CAST(ses.store_id AS STRING)                                      AS store_id,
    CAST(ses.store_name AS STRING)                                    AS store_name,
    CAST(ses.city AS STRING)                                          AS city,
    CAST(ses.store_type AS STRING)                                    AS store_type,
    CAST(ses.category AS STRING)                                      AS category,
    SUM(CAST(ses.sale_amount AS DECIMAL(38, 10)))                     AS total_revenue,
    SUM(CAST(ses.quantity AS INT))                                    AS total_quantity,
    COUNT(DISTINCT CAST(ses.transaction_id AS STRING))                AS transaction_count
FROM sales_enriched_silver ses
GROUP BY
    CAST(ses.sales_date AS DATE),
    CAST(ses.store_id AS STRING),
    CAST(ses.store_name AS STRING),
    CAST(ses.city AS STRING),
    CAST(ses.store_type AS STRING),
    CAST(ses.category AS STRING)
""")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_daily_store_category_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_store_category_sales.csv")
)

job.commit()
```