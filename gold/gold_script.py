```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Helpful CSV defaults (adjust if your silver differs)
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
}

csv_write_options = {
    "header": "true",
}

# -----------------------------------------------------------------------------------
# 1) Read Source Tables from S3 (Silver)
#    IMPORTANT: Must follow .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

store_daily_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/store_daily_sales_silver.{FILE_FORMAT}/")
)

product_daily_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/product_daily_sales_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create Temp Views
# -----------------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
store_daily_sales_silver_df.createOrReplaceTempView("store_daily_sales_silver")
product_daily_sales_silver_df.createOrReplaceTempView("product_daily_sales_silver")

# ===================================================================================
# TARGET TABLE: gold_sales_transaction_fact
# ===================================================================================

# 3) SQL Transform (includes ROW_NUMBER for dedup)
gold_sales_transaction_fact_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(sts.transaction_id AS STRING)               AS transaction_id,
            CAST(sts.transaction_timestamp AS TIMESTAMP)     AS transaction_timestamp,
            CAST(sts.transaction_date AS DATE)               AS transaction_date,
            CAST(sts.store_id AS STRING)                     AS store_id,
            CAST(sts.product_id AS STRING)                   AS product_id,
            CAST(sts.quantity AS INT)                        AS quantity,
            CAST(sts.unit_price AS DECIMAL(38,10))           AS unit_price,
            CAST(sts.gross_sales_amount AS DECIMAL(38,10))   AS gross_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY sts.transaction_id, sts.store_id, sts.product_id
                ORDER BY sts.transaction_timestamp DESC
            ) AS rn
        FROM sales_transactions_silver sts
    )
    SELECT
        transaction_id,
        transaction_timestamp,
        transaction_date,
        store_id,
        product_id,
        quantity,
        unit_price,
        gross_sales_amount
    FROM ranked
    WHERE rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_sales_transaction_fact_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_sales_transaction_fact.csv")
)

# ===================================================================================
# TARGET TABLE: gold_product_dim
# ===================================================================================

# 3) SQL Transform
gold_product_dim_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)                  AS product_id,
        CAST(ps.product_name AS STRING)                AS product_name,
        CAST(ps.category_name_standardized AS STRING)  AS category_name_standardized
    FROM products_silver ps
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_product_dim_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_product_dim.csv")
)

# ===================================================================================
# TARGET TABLE: gold_store_dim
# ===================================================================================

# 3) SQL Transform
gold_store_dim_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)      AS store_id,
        CAST(ss.store_name AS STRING)    AS store_name,
        CAST(ss.region AS STRING)        AS region
    FROM stores_silver ss
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_store_dim_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_store_dim.csv")
)

# ===================================================================================
# TARGET TABLE: gold_store_daily_sales_agg
# ===================================================================================

# 3) SQL Transform
gold_store_daily_sales_agg_df = spark.sql(
    """
    SELECT
        CAST(sdss.store_id AS STRING)                     AS store_id,
        CAST(sdss.transaction_date AS DATE)               AS transaction_date,
        CAST(sdss.total_transactions AS INT)              AS total_transactions,
        CAST(sdss.total_units_sold AS INT)                AS total_units_sold,
        CAST(sdss.total_gross_sales_amount AS DECIMAL(38,10)) AS total_gross_sales_amount
    FROM store_daily_sales_silver sdss
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_store_daily_sales_agg_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_store_daily_sales_agg.csv")
)

# ===================================================================================
# TARGET TABLE: gold_product_daily_sales_agg
# ===================================================================================

# 3) SQL Transform
gold_product_daily_sales_agg_df = spark.sql(
    """
    SELECT
        CAST(pdss.product_id AS STRING)                   AS product_id,
        CAST(pdss.transaction_date AS DATE)               AS transaction_date,
        CAST(pdss.total_transactions AS INT)              AS total_transactions,
        CAST(pdss.total_units_sold AS INT)                AS total_units_sold,
        CAST(pdss.total_gross_sales_amount AS DECIMAL(38,10)) AS total_gross_sales_amount
    FROM product_daily_sales_silver pdss
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    gold_product_daily_sales_agg_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_product_daily_sales_agg.csv")
)

job.commit()
```