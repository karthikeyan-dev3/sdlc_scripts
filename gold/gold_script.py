import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
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

monthly_sales_perf_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/monthly_sales_perf_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
daily_sales_agg_silver_df.createOrReplaceTempView("daily_sales_agg_silver")
monthly_sales_perf_silver_df.createOrReplaceTempView("monthly_sales_perf_silver")

# ------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------

# gold_products (gp) <- silver.products_silver (ps)
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)      AS product_id,
        CAST(ps.product_name AS STRING)    AS product_name,
        CAST(ps.product_category AS STRING) AS product_category,
        CAST(ps.price AS FLOAT)            AS price
    FROM products_silver ps
    """
)

# gold_stores (gs) <- silver.stores_silver (ss)
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)        AS store_id,
        CAST(ss.store_name AS STRING)      AS store_name,
        CAST(ss.store_location AS STRING)  AS store_location,
        CAST(ss.store_region AS STRING)    AS store_region
    FROM stores_silver ss
    """
)

# gold_sales (gsa) <- silver.sales_transactions_silver (sts) JOIN products_silver (ps) JOIN stores_silver (ss)
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        DATE(sts.sale_date)                AS sale_date,
        CAST(sts.quantity_sold AS INT)     AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    """
)

# gold_aggregated_sales (gas) <- silver.daily_sales_agg_silver (dsas)
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(dsas.store_id AS STRING)          AS store_id,
        CAST(dsas.product_id AS STRING)        AS product_id,
        DATE(dsas.sales_date)                  AS sales_date,
        CAST(dsas.total_quantity_sold AS INT)  AS total_quantity_sold,
        CAST(dsas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM daily_sales_agg_silver dsas
    """
)

# gold_sales_performance (gsp) <- silver.monthly_sales_perf_silver (msps)
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(msps.product_id AS STRING)            AS product_id,
        CAST(msps.store_id AS STRING)              AS store_id,
        CAST(msps.monthly_quantity_sold AS INT)    AS total_quantity_sold_last_month,
        CAST(msps.monthly_sales_amount AS DOUBLE)  AS total_sales_amount_last_month
    FROM monthly_sales_perf_silver msps
    """
)

# ------------------------------------------------------------
# 4) Save outputs (single CSV file per target, directly under TARGET_PATH)
# ------------------------------------------------------------
(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)