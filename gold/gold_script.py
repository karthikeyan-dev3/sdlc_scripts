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

# ----------------------------
# 1) READ SOURCE TABLES (S3)
# ----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
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

# ----------------------------
# 2) CREATE TEMP VIEWS
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# TARGET TABLE: gold_sales_data
# ============================================================
gold_sales_data_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)          AS transaction_id,
        CAST(sts.store_id AS STRING)                AS store_id,
        CAST(ss.store_name AS STRING)               AS store_name,
        CAST(sts.product_id AS STRING)              AS product_id,
        CAST(ps.product_name AS STRING)             AS product_name,
        CAST(sts.transaction_time AS TIMESTAMP)     AS sale_date,
        CAST(sts.quantity AS INT)                   AS quantity_sold,
        CAST(sts.sale_amount AS DOUBLE)             AS total_revenue
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_data.csv")
)

# ============================================================
# TARGET TABLE: gold_aggregated_sales
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.transaction_time AS TIMESTAMP) AS day,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity_sold,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(
            SUM(CAST(sts.sale_amount AS DOUBLE)) / COUNT(CAST(sts.sale_amount AS DOUBLE))
            AS DOUBLE
        ) AS average_transaction_value
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.store_id AS STRING),
        CAST(sts.product_id AS STRING),
        CAST(sts.transaction_time AS TIMESTAMP)
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ============================================================
# TARGET TABLE: gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(ss.store_name AS STRING) AS store_name,
            CAST(sts.transaction_time AS TIMESTAMP) AS day,
            CAST(sts.transaction_id AS STRING) AS transaction_id,
            CAST(sts.sale_amount AS DOUBLE) AS sale_amount,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(sts.quantity AS INT) AS quantity
        FROM sales_transactions_silver sts
        INNER JOIN stores_silver ss
            ON sts.store_id = ss.store_id
    ),
    store_day AS (
        SELECT
            store_id,
            store_name,
            day,
            CAST(COUNT(transaction_id) AS STRING) AS total_daily_transactions,
            CAST(SUM(sale_amount) AS DOUBLE) AS total_daily_revenue
        FROM base
        GROUP BY store_id, store_name, day
    ),
    prod_day AS (
        SELECT
            store_id,
            day,
            product_id,
            SUM(quantity) AS total_product_quantity
        FROM base
        GROUP BY store_id, day, product_id
    ),
    ranked AS (
        SELECT
            store_id,
            day,
            product_id AS top_selling_products,
            ROW_NUMBER() OVER (
                PARTITION BY store_id, day
                ORDER BY total_product_quantity DESC, product_id ASC
            ) AS rn
        FROM prod_day
    )
    SELECT
        sd.store_id,
        sd.store_name,
        sd.total_daily_transactions,
        sd.total_daily_revenue,
        r.top_selling_products
    FROM store_day sd
    INNER JOIN ranked r
        ON sd.store_id = r.store_id
       AND sd.day = r.day
    WHERE r.rn = 1
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# TARGET TABLE: gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
    WITH product_totals AS (
        SELECT
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(ps.product_name AS STRING) AS product_name,
            CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity_sold,
            CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue_contribution
        FROM sales_transactions_silver sts
        INNER JOIN products_silver ps
            ON sts.product_id = ps.product_id
        GROUP BY
            CAST(sts.product_id AS STRING),
            CAST(ps.product_name AS STRING)
    )
    SELECT
        product_id,
        product_name,
        total_quantity_sold,
        total_revenue_contribution,
        CAST(
            ROW_NUMBER() OVER (
                ORDER BY total_revenue_contribution DESC, total_quantity_sold DESC, product_id ASC
            ) AS DOUBLE
        ) AS performance_rank
    FROM product_totals
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()