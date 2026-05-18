import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

product_revenue_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_revenue_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

store_revenue_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_revenue_silver.{FILE_FORMAT}/")
)

daily_store_category_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_store_category_sales_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
product_revenue_silver_df.createOrReplaceTempView("product_revenue_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
store_revenue_silver_df.createOrReplaceTempView("store_revenue_silver")
daily_store_category_sales_silver_df.createOrReplaceTempView("daily_store_category_sales_silver")

# ============================================================
# Target: gold.gold_sales_performance (gsp)
# ============================================================
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)                 AS transaction_id,
        CAST(sts.product_id AS STRING)                     AS product_id,
        CAST(sts.store_id AS STRING)                       AS store_id,
        CAST(sts.quantity_sold AS INT)                     AS quantity_sold,
        CAST(sts.sale_date AS DATE)                        AS sale_date,
        CAST(sts.total_revenue AS DOUBLE)                  AS total_revenue,
        CAST(ps.category AS STRING)                        AS category_performance
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ============================================================
# Target: gold.gold_product_details (gpd)
# ============================================================
gold_product_details_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)                                                   AS product_id,
        CAST(ps.product_name AS STRING)                                                 AS product_name,
        CAST(ps.category AS STRING)                                                     AS category,
        CAST(ps.price AS FLOAT)                                                         AS price,
        CAST(prs.total_revenue AS DOUBLE) / SUM(CAST(prs.total_revenue AS DOUBLE)) OVER () AS revenue_contribution
    FROM products_silver ps
    LEFT JOIN product_revenue_silver prs
        ON ps.product_id = prs.product_id
    """
)

(
    gold_product_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_details.csv")
)

# ============================================================
# Target: gold.gold_store_details (gsd)
# ============================================================
gold_store_details_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)                AS store_id,
        CAST(ss.store_name AS STRING)              AS store_name,
        CONCAT(CAST(ss.city AS STRING), ', ', CAST(ss.state AS STRING)) AS location,
        CAST(ss.store_type AS STRING)              AS store_type,
        CAST(srs.total_revenue AS DOUBLE)          AS total_revenue
    FROM stores_silver ss
    LEFT JOIN store_revenue_silver srs
        ON ss.store_id = srs.store_id
    """
)

(
    gold_store_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_details.csv")
)

# ============================================================
# Target: gold.gold_aggregated_reports (gar)
# ============================================================
gold_aggregated_reports_df = spark.sql(
    """
    SELECT
        CAST(dscs.store_id AS STRING)            AS store_id,
        CAST(dscs.category AS STRING)            AS category,
        CAST(dscs.total_quantity_sold AS INT)    AS total_quantity_sold,
        CAST(dscs.total_revenue AS DOUBLE)       AS total_revenue,
        CAST(dscs.report_date AS DATE)           AS report_date
    FROM daily_store_category_sales_silver dscs
    """
)

(
    gold_aggregated_reports_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_reports.csv")
)

job.commit()