import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Read Source Tables from S3
# -----------------------------
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

# -----------------------------
# Create Temp Views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# Target: gold.gold_sales_aggregated
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.date AS DATE)             AS date,
        CAST(sts.revenue AS DOUBLE)        AS revenue,
        CAST(sts.quantity AS INT)          AS quantity
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ============================================================
# Target: gold.gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(ss.store_name AS STRING)       AS store_name,
        CAST(ss.city AS STRING)             AS city,
        CAST(ss.store_type AS STRING)       AS store_type,
        CAST(sts.date AS DATE)              AS reporting_date,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT)  AS transaction_count,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)      AS quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        sts.store_id,
        ss.store_name,
        ss.city,
        ss.store_type,
        sts.date
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold.gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
    WITH product_agg AS (
        SELECT
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(ps.product_name AS STRING) AS product_name,
            CAST(ps.category AS STRING) AS category,
            CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS revenue_contribution,
            CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS quantity_sold
        FROM sales_transactions_silver sts
        INNER JOIN products_silver ps
            ON sts.product_id = ps.product_id
        GROUP BY
            sts.product_id,
            ps.product_name,
            ps.category
    ),
    category_agg AS (
        SELECT
            category,
            CAST(SUM(revenue_contribution) AS DOUBLE) AS category_revenue,
            CAST(SUM(quantity_sold) AS INT) AS category_quantity
        FROM product_agg
        GROUP BY category
    ),
    ranked AS (
        SELECT
            pa.product_id,
            pa.product_name,
            pa.category,
            pa.revenue_contribution,
            pa.quantity_sold,
            CAST(
                CONCAT(
                    pa.category, ',',
                    CAST(ca.category_revenue AS STRING), ',',
                    CAST(ca.category_quantity AS STRING)
                ) AS STRING
            ) AS category_performance,
            CAST(RANK() OVER (ORDER BY pa.revenue_contribution DESC) AS STRING) AS top_performing_flag
        FROM product_agg pa
        INNER JOIN category_agg ca
            ON pa.category = ca.category
    )
    SELECT
        product_id,
        product_name,
        category,
        revenue_contribution,
        quantity_sold,
        category_performance,
        top_performing_flag
    FROM ranked
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()
