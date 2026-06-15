```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = Gluext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_silver_df.createOrReplaceTempView("product_silver")
store_silver_df.createOrReplaceTempView("store_silver")

# --------------------------------------------------------------------------------------
# Target: gold_sales_transactions
# --------------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS sale_id,
        CAST(sts.transaction_time AS TIMESTAMP) AS sale_datetime,
        DATE(CAST(sts.transaction_time AS TIMESTAMP)) AS sale_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity AS INT) AS quantity_sold,
        CAST(ps.price AS FLOAT) AS unit_price,
        CAST((CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS gross_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN product_silver ps
        ON sts.product_id = ps.product_id
    LEFT JOIN store_silver ss
        ON sts.store_id = ss.store_id
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_product
# --------------------------------------------------------------------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.brand AS STRING) AS brand,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.is_active AS BOOLEAN) AS is_active
    FROM product_silver ps
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_store
# --------------------------------------------------------------------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.state AS STRING) AS state_province,
        CAST(ss.open_date AS DATE) AS open_date
    FROM store_silver ss
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_daily_sales_store_product
# --------------------------------------------------------------------------------------
gold_daily_sales_store_product_df = spark.sql(
    """
    SELECT
        DATE(CAST(sts.transaction_time AS TIMESTAMP)) AS sales_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_units_sold,
        CAST(SUM(CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS gross_sales_amount,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS transaction_count
    FROM sales_transactions_silver sts
    LEFT JOIN product_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        DATE(CAST(sts.transaction_time AS TIMESTAMP)),
        CAST(sts.store_id AS STRING),
        CAST(sts.product_id AS STRING)
    """
)

(
    gold_daily_sales_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_store_product.csv")
)

# --------------------------------------------------------------------------------------
# Target: gold_data_refresh_status
# --------------------------------------------------------------------------------------
gold_data_refresh_status_df = spark.sql(
    """
    SELECT
        CAST(MAX(CAST(sts.transaction_time AS TIMESTAMP)) AS TIMESTAMP) AS source_max_extract_ts
    FROM sales_transactions_silver sts
    """
)

(
    gold_data_refresh_status_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_refresh_status.csv")
)

job.commit()
```