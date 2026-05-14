
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
# 1) Read source tables from S3
# ----------------------------
sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
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
data_quality_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

# ============================================================
# Target: gold_sales
# ============================================================
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ses.transaction_id AS STRING) AS transaction_id,
        CAST(ses.store_id AS STRING) AS store_id,
        CAST(ses.product_id AS STRING) AS product_id,
        DATE(ses.transaction_date) AS transaction_date,
        CAST(ses.quantity_sold AS INT) AS quantity_sold,
        CAST(ses.total_revenue AS DOUBLE) AS total_revenue
    FROM sales_enriched_silver ses
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target: gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ses.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.region AS STRING) AS region,
        CAST(SUM(CAST(ses.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(ses.transaction_id) AS INT) AS total_transactions,
        CAST(
            SUM(CAST(ses.total_revenue AS DOUBLE)) / COUNT(ses.transaction_id)
            AS DOUBLE
        ) AS average_transaction_value
    FROM sales_enriched_silver ses
    INNER JOIN stores_silver ss
        ON ses.store_id = ss.store_id
    GROUP BY
        ses.store_id,
        ss.store_name,
        ss.region
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
# Target: gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ses.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(SUM(CAST(ses.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(ses.quantity_sold AS INT)) AS INT) AS units_sold,
        CAST(
            SUM(CAST(ses.total_revenue AS DOUBLE))
            / SUM(SUM(CAST(ses.total_revenue AS DOUBLE))) OVER ()
            AS DOUBLE
        ) AS revenue_contribution
    FROM sales_enriched_silver ses
    INNER JOIN products_silver ps
        ON ses.product_id = ps.product_id
    GROUP BY
        ses.product_id,
        ps.product_name,
        ps.category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_aggregated_data
# ============================================================
gold_aggregated_data_df = spark.sql(
    """
    SELECT
        DATE(ses.transaction_date) AS aggregation_date,
        CAST(SUM(CAST(ses.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(ses.transaction_id) AS INT) AS total_transactions,
        CAST(SUM(CAST(ses.quantity_sold AS INT)) AS INT) AS total_units_sold,
        CAST(
            SUM(CAST(ses.total_revenue AS DOUBLE)) / COUNT(ses.transaction_id)
            AS DOUBLE
        ) AS average_transaction_value
    FROM sales_enriched_silver ses
    GROUP BY
        DATE(ses.transaction_date)
    """
)

(
    gold_aggregated_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_data.csv")
)

# ============================================================
# Target: gold_refresh_metadata
# ============================================================
gold_refresh_metadata_df = spark.sql(
    """
    SELECT
        DATE(dq.refresh_date) AS refresh_date,
        CAST(dq.data_quality_score AS DOUBLE) AS data_quality_score,
        CAST(dq.data_refresh_rate AS INT) AS data_refresh_rate
    FROM data_quality_daily_silver dq
    """
)

(
    gold_refresh_metadata_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_refresh_metadata.csv")
)

job.commit()