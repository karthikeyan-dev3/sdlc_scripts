import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
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
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

daily_sales_agg_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_agg_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
daily_sales_agg_silver_df.createOrReplaceTempView("daily_sales_agg_silver")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------------------

# gold.gold_products
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS price,
        CAST(ps.brand AS STRING)        AS brand
    FROM products_silver ps
    """
)

# gold.gold_stores
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)   AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING)   AS location,
        CAST(ss.store_type AS STRING) AS store_type
    FROM stores_silver ss
    """
)

# gold.gold_sales
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        DATE(sts.sale_date)                AS sale_date,
        CAST(sts.quantity_sold AS INT)     AS quantity_sold,
        CAST(sts.total_amount AS DOUBLE)   AS total_amount
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

# gold.aggregated_sales
aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(dsas.aggregation_date)                  AS aggregation_date,
        CAST(dsas.total_sales AS DOUBLE)             AS total_sales,
        CAST(dsas.total_transactions AS BIGINT)      AS total_transactions,
        CAST(dsas.average_transaction_value AS DOUBLE) AS average_transaction_value,
        CAST(dsas.category_sales_breakdown AS STRING)  AS category_sales_breakdown,
        CAST(dsas.store_sales_ranking AS STRING)       AS store_sales_ranking
    FROM daily_sales_agg_silver dsas
    """
)

# ------------------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------------------
(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

(
    aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales.csv")
)

job.commit()