```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue / Spark setup
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Source: silver.sales_transactions_silver (read once; used by all targets)
# NOTE: Path MUST be constructed using FILE_FORMAT and end with a trailing slash
# --------------------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

sts_df.createOrReplaceTempView("sales_transactions_silver")

# ======================================================================================
# TARGET 1: gold.gold_store_daily_sales
# ======================================================================================
gold_store_daily_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                   AS sales_date,
        CAST(sts.store_id AS STRING)                                   AS store_id,
        CAST(sts.store_name AS STRING)                                 AS store_name,
        CAST(sts.city AS STRING)                                       AS city,
        CAST(sts.state AS STRING)                                      AS state,
        CAST(sts.country AS STRING)                                    AS country,
        CAST(sts.store_type AS STRING)                                 AS store_type,
        CAST(SUM(CAST(sts.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                    AS total_quantity_sold,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT) AS total_transactions
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(sts.store_name AS STRING),
        CAST(sts.city AS STRING),
        CAST(sts.state AS STRING),
        CAST(sts.country AS STRING),
        CAST(sts.store_type AS STRING)
    """
)

(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# ======================================================================================
# TARGET 2: gold.gold_product_daily_sales
# ======================================================================================
gold_product_daily_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                   AS sales_date,
        CAST(sts.product_id AS STRING)                                 AS product_id,
        CAST(sts.product_name AS STRING)                               AS product_name,
        CAST(sts.brand AS STRING)                                      AS brand,
        CAST(sts.category AS STRING)                                   AS category,
        CAST(SUM(CAST(sts.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                    AS total_quantity_sold,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT) AS total_transactions
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(sts.product_id AS STRING),
        CAST(sts.product_name AS STRING),
        CAST(sts.brand AS STRING),
        CAST(sts.category AS STRING)
    """
)

(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

# ======================================================================================
# TARGET 3: gold.gold_store_category_daily_sales
# ======================================================================================
gold_store_category_daily_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS DATE)                                   AS sales_date,
        CAST(sts.store_id AS STRING)                                   AS store_id,
        CAST(sts.store_name AS STRING)                                 AS store_name,
        CAST(sts.city AS STRING)                                       AS city,
        CAST(sts.store_type AS STRING)                                 AS store_type,
        CAST(sts.category AS STRING)                                   AS category,
        CAST(SUM(CAST(sts.sale_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)                    AS total_quantity_sold,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS INT) AS total_transactions
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.sales_date AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(sts.store_name AS STRING),
        CAST(sts.city AS STRING),
        CAST(sts.store_type AS STRING),
        CAST(sts.category AS STRING)
    """
)

(
    gold_store_category_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_category_daily_sales.csv")
)

job.commit()
```